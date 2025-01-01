package ipfs

import (
	"context"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	"io"
)

type Store PoolClient

func (s *Store) Add(ctx context.Context, reader io.ReadCloser) (path.ImmutablePath, error) {
	return s.api.Unixfs().Add(ctx, files.NewReaderFile(reader))
}
func (s *Store) Get(ctx context.Context, cidStr string, offset, length int64) (io.ReadCloser, error) {
	meatCid, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}
	//f, err := s.api.Unixfs().Get(ctx, path.IpfsPath(meatCid))
	resp, err := s.api.Request("cat", meatCid.String()).
		Option("offset", offset).
		Option("length", length).
		Send(ctx)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, err
	}
	//defer resp.Output.Close()
	return resp.Output, nil
}
