package ipfs

import (
	"bytes"
	"context"
	dagpoolcli "github.com/filedag-project/filedag-storage/dag/pool/client"
	"github.com/ipfs/boxo/coreiface/options"
	"github.com/ipfs/boxo/coreiface/path"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/kubo/client/rpc"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"golang.org/x/xerrors"
	"strings"
)

var log = logging.Logger("ipfs-client")
var _ dagpoolcli.PoolClient = (*PoolClient)(nil)

type PoolClient struct {
	sh        *rpc.HttpApi
	addr      string
	enablePin bool
}

// NewPoolClient new a dagPoolClient
func NewPoolClient(addr string, enablePin bool) (*PoolClient, error) {
	sh, err := rpc.NewApi(ma.StringCast(addr))
	return &PoolClient{
		sh:        sh,
		addr:      "",
		enablePin: enablePin,
	}, err
}
func (i *PoolClient) Close(ctx context.Context) {
	return
}
func (i *PoolClient) DeleteBlock(ctx context.Context, cid cid.Cid) error {
	return nil
}

func (i *PoolClient) Has(ctx context.Context, cid cid.Cid) (bool, error) {
	_, err := i.GetSize(ctx, cid)
	if err != nil {
		if xerrors.Is(err, format.ErrNotFound{Cid: cid}) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func (i *PoolClient) Get(ctx context.Context, cid cid.Cid) (blocks.Block, error) {
	log.Debugf(cid.String())
	node, err := i.sh.Dag().Get(ctx, cid)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, format.ErrNotFound{Cid: cid}
		}
		return nil, err
	}
	return node, nil
}

func (i *PoolClient) GetSize(ctx context.Context, cid cid.Cid) (int, error) {
	log.Debugf(cid.String())
	stat, err := i.sh.Block().Stat(ctx, path.IpfsPath(cid))
	return stat.Size(), err
}

func (i *PoolClient) Put(ctx context.Context, block blocks.Block) error {
	cidBuilder, _ := merkledag.PrefixForCidVersion(0)
	cidCodec := multicodec.Code(cidBuilder.Codec).String()
	_, err := i.sh.Block().Put(ctx, bytes.NewReader(block.RawData()),
		options.Block.Hash(cidBuilder.MhType, cidBuilder.MhLength),
		options.Block.CidCodec(cidCodec),
		options.Block.Format("v0"))
	return err
}

func (i *PoolClient) PutMany(ctx context.Context, blocks []blocks.Block) error {
	for _, block := range blocks {
		if err := i.Put(ctx, block); err != nil {
			return err
		}
	}
	return nil
}

func (i *PoolClient) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {
	//TODO implement me
	panic("implement me")
}

func (i *PoolClient) HashOnRead(enabled bool) {
	//TODO implement me
	panic("implement me")
}
