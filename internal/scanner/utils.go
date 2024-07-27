package scanner

import (
	"context"
	"fmt"

	"github.com/xssnick/tonutils-go/ton"
)

func (s *Scanner) getShardID(shard *ton.BlockIDExt) string {
	return fmt.Sprintf("%d|%d", shard.Workchain, shard.Shard)
}

func (s *Scanner) getNotSeenShards(ctx context.Context, shard *ton.BlockIDExt) (ret []*ton.BlockIDExt, err error) {
	// если последний сохранённый блок в памяти совпадает с переданным блоком - ничего делать не нужно
	if seqno, ok := s.shardLastSeqNo[s.getShardID(shard)]; ok && seqno == shard.SeqNo {
		return nil, nil
	}

	// достаю блок
	block, err := s.api.GetBlockData(ctx, shard)
	if err != nil {
		return nil, fmt.Errorf("failed to get block data: %w", err)
	}

	// достаю родительские блоки
	parents, err := block.BlockInfo.GetParentBlocks()
	if err != nil {
		return nil, fmt.Errorf("failed to get parent blocks (%d:%d): %w",
			shard.Workchain, shard.Shard, err)
	}

	// прохожусь по родительским блокам
	for _, parent := range parents {
		// делаю ту же операцию рекурсивно для каждого родителя
		notSeenShards, err := s.getNotSeenShards(ctx, parent)
		if err != nil {
			return nil, err
		}
		// добавляю в слайс неучтенных шардов
		ret = append(ret, notSeenShards...)
	}
	ret = append(ret, shard)

	return ret, nil
}

func (s *Scanner) addBlock(master *ton.BlockIDExt) error {
	s.lastBlock.SeqNo++
	return nil
}

func (s *Scanner) getLastBlockSeqno(ctx context.Context) (uint32, error) {
	lastMaster, err := s.api.GetMasterchainInfo(ctx)
	if err != nil {
		return 0, err
	}

	return lastMaster.SeqNo, nil
}
