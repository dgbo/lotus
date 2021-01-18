package cli

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/filecoin-project/lotus/lib/parmap"

	"github.com/filecoin-project/go-address"

	"github.com/filecoin-project/lotus/chain/actors"

	miner3 "github.com/filecoin-project/specs-actors/v3/actors/builtin/miner"

	"github.com/filecoin-project/go-state-types/big"
	lapi "github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
	builtin3 "github.com/filecoin-project/specs-actors/v3/actors/builtin"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/chain/store"
	"github.com/urfave/cli/v2"
)

var chainDisputeSetCmd = &cli.Command{
	Name:  "disputer",
	Usage: "interact with the window post disputer",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "max-fee",
			Usage: "Spend up to X attoFIL for DisputeWindowedPoSt message(s)",
		},
		&cli.StringFlag{
			Name:  "from",
			Usage: "optionally specify the account to send funds from",
		},
	},
	Subcommands: []*cli.Command{
		disputerStartCmd,
		disputerMsgCmd,
	},
}

var disputerMsgCmd = &cli.Command{
	Name:      "dispute",
	Usage:     "Send a specific DisputeWindowedPoSt message",
	ArgsUsage: "[minerAddress deadline postIndex]",
	Flags:     []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		if cctx.NArg() != 3 {
			fmt.Println("Usage: findpeer [minerAddress deadline postIndex]")
			return nil
		}

		ctx := ReqContext(cctx)

		api, closer, err := GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		toa, err := address.NewFromString(cctx.Args().First())
		if err != nil {
			return fmt.Errorf("given 'to' address %q was invalid: %w", cctx.Args().First(), err)
		}

		deadline, err := strconv.ParseUint(cctx.Args().Get(1), 10, 64)
		if err != nil {
			return err
		}

		postIndex, err := strconv.ParseUint(cctx.Args().Get(2), 10, 64)
		if err != nil {
			return err
		}

		var fromAddr address.Address
		if from := cctx.String("from"); from == "" {
			defaddr, err := api.WalletDefaultAddress(ctx)
			if err != nil {
				return err
			}

			fromAddr = defaddr
		} else {
			addr, err := address.NewFromString(from)
			if err != nil {
				return err
			}

			fromAddr = addr
		}

		dpp, aerr := actors.SerializeParams(&miner3.DisputeWindowedPoStParams{
			Deadline:  deadline,
			PoStIndex: postIndex,
		})

		if aerr != nil {
			return xerrors.Errorf("failed to serailize params: %w", aerr)
		}

		dmsg := &types.Message{
			To:     toa,
			From:   fromAddr,
			Value:  big.Zero(),
			Method: builtin3.MethodsMiner.DisputeWindowedPoSt,
			Params: dpp,
		}

		rslt, err := api.StateCall(ctx, dmsg, types.EmptyTSK)
		if err != nil {
			return xerrors.Errorf("failed to simulate dispute: %w", err)
		}

		var mss *lapi.MessageSendSpec
		if cctx.IsSet("max-fee") {
			maxFee, err := types.BigFromString(cctx.String("max-fee"))
			if err != nil {
				return fmt.Errorf("parsing max-fee: %w", err)
			}
			mss = &lapi.MessageSendSpec{
				MaxFee: maxFee,
			}
		}

		if rslt.MsgRct.ExitCode == 0 {
			sm, err := api.MpoolPushMessage(ctx, dmsg, mss)
			if err != nil {
				return err
			}

			fmt.Println("dispute message ", sm.Cid())
		} else {
			fmt.Println("dispute is unsuccessful")
		}

		return nil
	},
}

var disputerStartCmd = &cli.Command{
	Name:      "start",
	Usage:     "Start the window post disputer",
	ArgsUsage: "[minerAddress]",
	Flags:     []cli.Flag{},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()

		ctx := ReqContext(cctx)

		var fromAddr address.Address
		if from := cctx.String("from"); from == "" {
			defaddr, err := api.WalletDefaultAddress(ctx)
			if err != nil {
				return err
			}

			fromAddr = defaddr
		} else {
			addr, err := address.NewFromString(from)
			if err != nil {
				return err
			}

			fromAddr = addr
		}

		headChanges, err := api.ChainNotify(ctx)
		if err != nil {
			return err
		}

		head, ok := <-headChanges
		if !ok {
			return xerrors.Errorf("Notify stream was invalid")
		}

		if len(head) != 1 {
			return xerrors.Errorf("Notify first entry should have been one item")
		}

		if head[0].Type != store.HCCurrent {
			return xerrors.Errorf("expected current head on Notify stream (got %s)", head[0].Type)
		}

		fmt.Println("checking sync status")

		if err := SyncWait(ctx, api, false); err != nil {
			return xerrors.Errorf("sync wait: %w", err)
		}

		fmt.Println("starting up window post disputer")

		var mss *lapi.MessageSendSpec
		if cctx.IsSet("max-fee") {
			maxFee, err := types.BigFromString(cctx.String("max-fee"))
			if err != nil {
				return fmt.Errorf("parsing max-fee: %w", err)
			}
			mss = &lapi.MessageSendSpec{
				MaxFee: maxFee,
			}
		}

		ticker := time.NewTicker(120 * time.Second)
		defer ticker.Stop()

		disputeLoop := func() (error, bool) {
			select {
			case notif, ok := <-headChanges:
				if !ok {
					return xerrors.Errorf("head change channel errored"), true
				}
				for _, val := range notif {
					switch val.Type {
					case store.HCApply:

						if val.Val.Height() < miner3.WPoStChallengeWindow {
							// it's too early in the chain to challenge any posts
							continue
						}

						// TODO: If the newly applied tipset is built on top of n null tipset(s), we should also check the tipsets WPoStChallengeWindow+[1...n]  epochs ago

						// Since we're waiting WPoStChallengeWindow epochs already, we don't have to worry about confidence of the message -- it's already pretty deep onchain
						ts, err := api.ChainGetTipSetByHeight(ctx, val.Val.Height()-miner3.WPoStChallengeWindow, val.Val.Key())
						if err != nil {
							return xerrors.Errorf("failed to get tipset 60 epochs ago: %w", err), false
						}

						// TODO: Build a cache of tipsets we've already inspected and continue here if appropriate

						pmsgs, err := api.ChainGetParentMessages(ctx, ts.Cids()[0])
						if err != nil {
							return xerrors.Errorf("failed to get parent messages: %w", err), false
						}

						wpmsgs, err := getSubmitWindowedPoSts(ctx, api, pmsgs, ts.Key())
						if err != nil {
							return xerrors.Errorf("failed to get post messages: %w", err), false
						}

						dpmsgs, err := makeDisputeWindowedPosts(ctx, api, wpmsgs, fromAddr)
						if err != nil {
							return xerrors.Errorf("failed to check for disputes: %w", err), false
						}

						for _, dpmsg := range dpmsgs {
							fmt.Println("disputing a post at height ", ts.Height())
							_, err := api.MpoolPushMessage(ctx, dpmsg, mss)
							if err != nil {
								return xerrors.Errorf("failed to dispute post message: %w", err), false
							}

							// TODO: Track / report on message landing on chain?
						}
					case store.HCRevert:
						// do nothing
					default:
						return xerrors.Errorf("unexpected head change type %s", val.Type), true
					}
				}
			case <-ticker.C:
				fmt.Print("Running health check: ")

				cctx, cancel := context.WithTimeout(ctx, 5*time.Second)

				if _, err := api.ID(cctx); err != nil {
					cancel()
					return xerrors.Errorf("health check failed"), true
				}

				cancel()

				fmt.Println("Node online")
			case <-ctx.Done():
				return nil, true
			}

			return nil, false
		}

		for {
			err, shutdown := disputeLoop()
			if err != nil && shutdown {
				fmt.Println("disputer errored, shutting down: ", err)
				break
			}

			if err != nil && !shutdown {
				fmt.Println("disputer errored, continuing to run: ", err)
			}

			if shutdown {
				fmt.Println("disputer shutting down")
				break
			}
		}

		return nil
	},
}

// for a list of messages and the tsk in which they were executed, returns the _successful_ SubmitWindowedPoSt msgs among them
func getSubmitWindowedPoSts(ctx context.Context, api lapi.FullNode, msgs []lapi.Message, tsk types.TipSetKey) ([]lapi.Message, error) {
	var lk sync.Mutex
	posts := make([]lapi.Message, 0)

	parmap.Par(50, msgs, func(msg lapi.Message) {
		if msg.Message.Method != builtin3.MethodsMiner.SubmitWindowedPoSt {
			return
		}

		// TODO: Might be worth building a cache of miner actors
		toActor, err := api.StateGetActor(ctx, msg.Message.To, types.EmptyTSK)
		if err != nil {
			return
		}

		if toActor.Code != builtin3.StorageMinerActorCodeID {
			return
		}

		rct, err := api.StateGetReceipt(ctx, msg.Cid, tsk)
		if err != nil {
			return
		}

		if rct.ExitCode != 0 {
			return
		}

		lk.Lock()
		posts = append(posts, msg)
		lk.Unlock()
	})

	return posts, nil
}

// for a list of successful SubmitWindowedPoSt msgs, tries disputing each of them
// returns a list of DisputeWindowedPoSt msgs that are expected to succeed if sent
func makeDisputeWindowedPosts(ctx context.Context, api lapi.FullNode, posts []lapi.Message, sender address.Address) ([]*types.Message, error) {
	disputes := make([]*types.Message, 0)
	for _, post := range posts {
		// TODO: Remove when ready to merge
		fmt.Println("Actually disputing posts")

		var postParams miner3.SubmitWindowedPoStParams

		if err := postParams.UnmarshalCBOR(bytes.NewReader(post.Message.Params)); err != nil {
			return nil, xerrors.Errorf("failed to unmarshal params: %w", err)
		}

		dpp, aerr := actors.SerializeParams(&miner3.DisputeWindowedPoStParams{
			Deadline: postParams.Deadline,
			// TODO: It's tricky (maybe impossible?) to find out exactly which index this post corresponds to
			// We could just try all indices (from 0 to the length of the OptimisticPoStSubmissionsSnapshot)
			PoStIndex: 0,
		})

		if aerr != nil {
			return nil, xerrors.Errorf("failed to serailize params: %w", aerr)
		}

		dispute := &types.Message{
			To:     post.Message.To,
			From:   sender,
			Value:  big.Zero(),
			Method: builtin3.MethodsMiner.DisputeWindowedPoSt,
			Params: dpp,
		}

		rslt, err := api.StateCall(ctx, dispute, types.EmptyTSK)
		if err == nil && rslt.MsgRct.ExitCode == 0 {
			// TODO: Remove when ready to merge
			fmt.Println("DISPUTE THIS!!")
			disputes = append(disputes, dispute)
		} else {
			// TODO: Remove when ready to merge
			fmt.Println("Sorry, can't dispute this")
		}

	}

	return disputes, nil
}
