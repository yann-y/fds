package ipfs

import (
	"context"
	dagpoolcli "github.com/filedag-project/filedag-storage/dag/pool/client"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	shell "github.com/ipfs/go-ipfs-api"
	format "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
	"strings"
)

var log = logging.Logger("ipfs-client")
var _ dagpoolcli.PoolClient = (*PoolClient)(nil)

type PoolClient struct {
	sh        *shell.Shell
	addr      string
	enablePin bool
}

// NewPoolClient new a dagPoolClient
func NewPoolClient(addr string, enablePin bool) (*PoolClient, error) {
	sh := shell.NewShell(addr)
	return &PoolClient{
		sh:        sh,
		addr:      "",
		enablePin: enablePin,
	}, nil
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
	get, err := i.sh.BlockGet(cid.String())
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, format.ErrNotFound{Cid: cid}
		}
		return nil, err
	}
	return blocks.NewBlock(get), nil
}

func (i *PoolClient) GetSize(ctx context.Context, cid cid.Cid) (int, error) {
	log.Debugf(cid.String())
	_, size, err := i.sh.BlockStat(cid.String())
	return size, err
}

func (i *PoolClient) Put(ctx context.Context, block blocks.Block) error {
	_, err := i.sh.BlockPut(block.RawData(), "v0", "sha2-256", -1)
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
