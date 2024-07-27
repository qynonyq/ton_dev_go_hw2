package scanner

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
	"github.com/xssnick/tonutils-go/tlb"
)

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
			return fmt.Errorf("failed to parse comment: %w", err)
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
			logrus.Infof("[JTN] %s jettons received from %s", amountFmt, sender)
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

		logrus.Infof("[JTN] %s jettons received from %s with comment %q", amountFmt, sender, comment)
	}

	return nil
}
