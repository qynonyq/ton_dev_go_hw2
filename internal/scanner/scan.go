package scanner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"golang.org/x/sync/errgroup"

	"github.com/qynonyq/ton_dev_go_hw2/internal/app"
	"github.com/qynonyq/ton_dev_go_hw2/internal/storage"
)

type Scanner struct {
	api            *ton.APIClient
	lastBlock      storage.Block
	shardLastSeqNo map[string]uint32
}

func NewScanner(ctx context.Context) (*Scanner, error) {
	client := liteclient.NewConnectionPool()

	if err := client.AddConnectionsFromConfigUrl(ctx, app.TestnetCfgURL); err != nil {
		return nil, err
	}

	api := ton.NewAPIClient(client)

	return &Scanner{
		api:            api,
		lastBlock:      storage.Block{},
		shardLastSeqNo: make(map[string]uint32),
	}, nil
}

func (s *Scanner) Listen(ctx context.Context) {
	logrus.Info("[SCN] start scanning blocks")

	if s.lastBlock.SeqNo == 0 {
		lastMaster, err := s.api.GetMasterchainInfo(ctx)
		for err != nil {
			time.Sleep(time.Second)
			logrus.Error("[SCN] failed to get last master: ", err)
			lastMaster, err = s.api.GetMasterchainInfo(ctx)
		}

		s.lastBlock.SeqNo = lastMaster.SeqNo
		s.lastBlock.Shard = lastMaster.Shard
		s.lastBlock.Workchain = lastMaster.Workchain
	}

	master, err := s.api.LookupBlock(
		ctx,
		s.lastBlock.Workchain,
		s.lastBlock.Shard,
		s.lastBlock.SeqNo,
	)
	for err != nil {
		logrus.Error("[SCN] failed to lookup master block: ", err)
		time.Sleep(time.Second)
		master, err = s.api.LookupBlock(
			ctx,
			s.lastBlock.Workchain,
			s.lastBlock.Shard,
			s.lastBlock.SeqNo,
		)
	}

	firstShards, err := s.api.GetBlockShardsInfo(ctx, master)
	for err != nil {
		logrus.Error("[SCN] failed to get first shards: ", err)
		time.Sleep(time.Second)
	}

	for _, shard := range firstShards {
		s.shardLastSeqNo[s.getShardID(shard)] = shard.SeqNo
	}

	s.processBlocks(ctx)
}

func (s *Scanner) processBlocks(ctx context.Context) {
	const (
		delayBase = 10 * time.Millisecond
		delayMax  = 8 * time.Second
	)
	delay := delayBase

	for {
		master, err := s.api.LookupBlock(
			ctx,
			s.lastBlock.Workchain,
			s.lastBlock.Shard,
			s.lastBlock.SeqNo,
		)
		if err == nil {
			delay = delayBase
			logrus.Debugf("[SCN] master block %d", master.SeqNo)
		}
		if err != nil {
			if !errors.Is(err, ton.ErrBlockNotFound) {
				logrus.Errorf("[SCN] failed to lookup master block %d: %s", s.lastBlock.SeqNo, err)
			}

			time.Sleep(delay)
			delay *= 2
			if delay > delayMax {
				delay = delayMax
			}

			continue
		}

		scanErr := s.processMcBlock(ctx, master)
		if scanErr != nil {
			logrus.Error("[SCN] mc block err: ", scanErr)
		}
	}
}

func (s *Scanner) processMcBlock(ctx context.Context, master *ton.BlockIDExt) error {
	start := time.Now()

	currentShards, err := s.api.GetBlockShardsInfo(ctx, master)
	if err != nil {
		return err
	}
	if len(currentShards) == 0 {
		logrus.Debugf("[SCN] block [%d] without shards", master.SeqNo)
		return nil
	}
	logrus.Debugf("[SCN] %d current shards ", len(currentShards))

	// duplicate check
	elems := make(map[uint32]struct{}, len(currentShards))
	duplicatesNum := 0
	for _, shard := range currentShards {
		if _, ok := elems[shard.SeqNo]; ok {
			duplicatesNum++
			continue
		}
		elems[shard.SeqNo] = struct{}{}
	}
	if duplicatesNum > 0 {
		logrus.Debugf("[SCN] %d duplicate current shards ", duplicatesNum)
	}
	//

	var newShards []*ton.BlockIDExt
	for _, shard := range currentShards {
		notSeen, err := s.getNotSeenShards(ctx, shard)
		if err != nil {
			return err
		}

		s.shardLastSeqNo[s.getShardID(shard)] = shard.SeqNo
		newShards = append(newShards, notSeen...)
	}
	if len(newShards) == 0 {
		newShards = currentShards
	} else {
		newShards = append(newShards, currentShards...)
	}
	logrus.Debugf("[SCN] %d new shards ", len(newShards))

	// duplicate check
	elems = make(map[uint32]struct{}, len(newShards))
	duplicatesNum = 0
	for _, shard := range newShards {
		if _, ok := elems[shard.SeqNo]; ok {
			duplicatesNum++
			continue
		}
		elems[shard.SeqNo] = struct{}{}
	}
	if duplicatesNum > 0 {
		logrus.Debugf("[SCN] %d duplicate new shards ", duplicatesNum)
	}
	//

	logrus.Debugf("[SCN] %d new shards after appending", len(newShards))

	// duplicate check
	elems = make(map[uint32]struct{}, len(newShards))
	duplicatesNum = 0
	for _, shard := range newShards {
		if _, ok := elems[shard.SeqNo]; ok {
			duplicatesNum++
			continue
		}
		elems[shard.SeqNo] = struct{}{}
	}
	if duplicatesNum > 0 {
		logrus.Debugf("[SCN] %d duplicate new shards after appending current shards", duplicatesNum)
	}
	//

	s.addBlock(master)

	return nil

	var (
		eg     errgroup.Group
		mu     sync.Mutex
		txList []*tlb.Transaction
	)
	for _, shard := range newShards {
		var (
			txsShort []ton.TransactionShortInfo
			after    *ton.TransactionID3
		)

		//for more {
		//	counter++

		// slice can have duplicated transactions, why?
		txsShort, _, err := s.api.GetBlockTransactionsV2(
			ctx,
			shard,
			100,
			after,
		)
		if err != nil {
			return err
		}
		if len(txsShort) > 0 {
			logrus.Debugf("[SCN] %d transactions", len(txsShort))
		}

		elems := make(map[uint64]struct{}, len(txsShort))
		duplicatesNum := 0
		for _, tx := range txsShort {
			if _, ok := elems[tx.LT]; ok {
				duplicatesNum++
			}
			elems[tx.LT] = struct{}{}
		}
		if duplicatesNum > 0 {
			logrus.Debugf("[SCN] found %d duplicate short transactions", duplicatesNum)
		}

		//if more {
		//	after = txsShort[len(txsShort)-1].ID3()
		//}

		for _, txShort := range txsShort {
			eg.Go(func() error {
				tx, err := s.api.GetTransaction(
					ctx,
					shard,
					address.NewAddress(0, 0, txShort.Account),
					txShort.LT,
				)
				if err != nil {
					logrus.Errorf("[SCN] failed to load tx: %s", err)
					return err
				}

				mu.Lock()
				txList = append(txList, tx)
				mu.Unlock()

				return nil
			})
		}
		//}

		//counter = 0
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("[SCN] failed to get transactions: %w", err)
	}

	// check txs for uniqueness
	elemsTxs := make(map[uint64]struct{}, len(txList))
	duplicatesNum = 0
	for _, tx := range txList {
		if _, ok := elemsTxs[tx.LT]; ok {
			duplicatesNum++
		}
		elemsTxs[tx.LT] = struct{}{}
	}
	if duplicatesNum > 0 {
		logrus.Debugf("[SCN] found %d duplicate transactions", duplicatesNum)
	}

	for _, tx := range txList {
		// use goroutines because sometimes there is a great amount of transactions
		eg.Go(func() error {
			if err := s.processTx(tx); err != nil {
				// add block, otherwise scanner stucks here trying to get same block
				s.addBlock(master)
				return err
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return err
	}

	s.addBlock(master)

	lastSeqno, err := s.getLastBlockSeqno(ctx)
	if err != nil {
		logrus.Infof("[SCN] block [%d] processed in [%.2fs] with [%d] transactions",
			master.SeqNo,
			time.Since(start).Seconds(),
			len(txList),
		)
	} else {
		logrus.Infof("[SCN] block [%d|%d] processed in [%.2fs] with [%d] transactions",
			master.SeqNo,
			lastSeqno,
			time.Since(start).Seconds(),
			len(txList),
		)
	}

	return nil
}
