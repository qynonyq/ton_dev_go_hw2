package scanner

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"golang.org/x/sync/errgroup"
)

func (s *Scanner) processBlocks(ctx context.Context) {
	const (
		// small delay for blocks with big batch of transactions
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
		}
		if err != nil {
			if !errors.Is(err, ton.ErrBlockNotFound) {
				logrus.Errorf("[SCN] failed to lookup master block %d: %s", s.lastBlock.SeqNo, err)
			}

			// exponential retry
			time.Sleep(delay)
			delay *= 2
			if delay > delayMax {
				delay = delayMax
			}

			continue
		}

		scanErr := s.processMcBlock(ctx, master)
		if scanErr != nil {
			logrus.Error("[SCN] failed to process MC block: ", scanErr)
		}
	}
}

func (s *Scanner) processMcBlock(ctx context.Context, master *ton.BlockIDExt) error {
	start := time.Now()

	currentShards, err := s.api.GetBlockShardsInfo(ctx, master)
	if err != nil {
		return err
	}

	shards := make(map[string]*ton.BlockIDExt, len(currentShards))

	for _, shard := range currentShards {
		// unique key
		key := fmt.Sprintf("%d:%d:%d", shard.Workchain, shard.Shard, shard.SeqNo)
		shards[key] = shard

		if err := s.fillWithNotSeenShards(ctx, shards, shard); err != nil {
			return err
		}
		s.lastShardsSeqNo[s.getShardID(shard)] = shard.SeqNo
	}

	var (
		eg  errgroup.Group
		txs []*tlb.Transaction
	)
	for _, shard := range shards {
		shardTxs, err := s.getTxsFromShard(ctx, shard)
		if err != nil {
			return err
		}
		txs = append(txs, shardTxs...)
	}

	for _, tx := range txs {
		// goroutines for big batch of transactions
		eg.Go(func() error {
			if err := s.processTx(tx); err != nil {
				// add block, otherwise scanner stucks here trying to get the same block
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
			len(txs),
		)
	} else {
		logrus.Infof("[SCN] block [%d|%d] processed in [%.2fs] with [%d] transactions",
			master.SeqNo,
			lastSeqno,
			time.Since(start).Seconds(),
			len(txs),
		)
	}

	return nil
}
func (s *Scanner) getTxsFromShard(ctx context.Context, shard *ton.BlockIDExt) ([]*tlb.Transaction, error) {
	var (
		after    *ton.TransactionID3
		more     = true
		err      error
		eg       errgroup.Group
		txsShort []ton.TransactionShortInfo
		mu       sync.Mutex
		txs      []*tlb.Transaction
	)

	for more {
		// problem: method can return duplicate transactions
		txsShort, more, err = s.api.GetBlockTransactionsV2(
			ctx,
			shard,
			100,
			after,
		)
		if err != nil {
			return nil, err
		}

		if more {
			after = txsShort[len(txsShort)-1].ID3()
		}

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
				txs = append(txs, tx)
				mu.Unlock()

				return nil
			})
		}
	}

	if err := eg.Wait(); err != nil {
		return nil, fmt.Errorf("[SCN] failed to get transactions: %w", err)
	}

	return txs, nil
}

func (s *Scanner) processTx(tx *tlb.Transaction) error {
	if tx.IO.In.MsgType != tlb.MsgTypeInternal {
		return nil
	}

	internalMsg := tx.IO.In.AsInternal()
	if internalMsg.Body == nil {
		return errors.New("empty body")
	}

	bodySlice := internalMsg.Body.BeginParse()
	amount := internalMsg.Amount.String()

	opcode, err := bodySlice.LoadUInt(32)
	if err != nil {
		if strings.Contains(err.Error(), "not enough data") {
			logrus.Infof("%s TON received from %s", amount, internalMsg.SenderAddr())
			return nil
		}
		return err
	}

	switch opcode {
	// text comment
	case 0:
		comment, err := bodySlice.LoadStringSnake()
		if err != nil {
			return fmt.Errorf("[TON] failed to parse comment: %w", err)
		}
		logrus.Infof("%s TON received from %s with comment %q", amount, internalMsg.SenderAddr(), comment)
	// jetton transfer notification
	case 0x7362d09c:
		// skip query_id
		if _, err := bodySlice.LoadUInt(64); err != nil {
			return fmt.Errorf("[JTN] failed to skip query_id: %w", err)
		}

		amount, err := bodySlice.LoadCoins()
		if err != nil {
			return fmt.Errorf("[JTN] failed to load coins: %s", err)
		}

		sender, err := bodySlice.LoadAddr()
		if err != nil {
			return fmt.Errorf("[JTN] failed to parse sender address: %w", err)
		}

		// divide amount by 1 billion
		amountFmt := strconv.FormatFloat(float64(amount)/1e9, 'f', -1, 32)

		fwdPayload, err := bodySlice.LoadMaybeRef()
		if err != nil {
			logrus.Warnf("[JTN] failed to parse forward payload: %s", err)
			return nil
		}
		if fwdPayload == nil {
			logrus.Infof("%s jettons received from %s", amountFmt, sender)
			return nil
		}

		opcode, err := fwdPayload.LoadUInt(32)
		if err != nil {
			return fmt.Errorf("[JTN] failed to parse forward payload opcode: %s", err)
		}
		if opcode != 0 {
			return nil
		}

		comment, err := fwdPayload.LoadStringSnake()
		if err != nil {
			return fmt.Errorf("[JTN] failed to parse forward payload comment: %s", err)
		}

		logrus.Infof("%s jettons received from %s with comment %q", amountFmt, sender, comment)
	}

	return nil
}
