package ipfs

import (
	"bytes"
	"context"
	"github.com/ipfs/boxo/coreiface/options"
	"github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/ipld/merkledag"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multicodec"
	"golang.org/x/xerrors"
	"strings"
)

type BlockAPI PoolClient

func (b *BlockAPI) Close(ctx context.Context) {
	return
}
func (b *BlockAPI) DeleteBlock(ctx context.Context, cid cid.Cid) error {
	return nil
}

func (b *BlockAPI) Has(ctx context.Context, cid cid.Cid) (bool, error) {
	_, err := b.GetSize(ctx, cid)
	if err != nil {
		if xerrors.Is(err, format.ErrNotFound{Cid: cid}) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (b *BlockAPI) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	log.Debugf(cid.String())
	node, err := b.api.Dag().Get(ctx, cid)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, format.ErrNotFound{Cid: cid}
		}
		return nil, err
	}
	return node, nil
}

func (b *BlockAPI) GetSize(ctx context.Context, cid cid.Cid) (int, error) {
	log.Debugf(cid.String())
	stat, err := b.api.Block().Stat(ctx, path.IpfsPath(cid))
	return stat.Size(), err
}

func (b *BlockAPI) Put(ctx context.Context, block blocks.Block) error {
	cidBuilder, _ := merkledag.PrefixForCidVersion(0)
	cidCodec := multicodec.Code(cidBuilder.Codec).String()
	_, err := b.api.Block().Put(ctx, bytes.NewReader(block.RawData()),
		options.Block.Hash(cidBuilder.MhType, cidBuilder.MhLength),
		options.Block.CidCodec(cidCodec),
		options.Block.Format("v0"))
	return err
}

func (b *BlockAPI) PutMany(ctx context.Context, blocks []blocks.Block) error {
	for _, block := range blocks {
		if err := b.Put(ctx, block); err != nil {
			return err
		}
	}
	return nil
}

func (b *BlockAPI) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	//TODO implement me
	panic("implement me")
}

func (b *BlockAPI) HashOnRead(enabled bool) {
	//TODO implement me
	panic("implement me")
}
