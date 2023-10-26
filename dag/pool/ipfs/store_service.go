package ipfs

import (
	"context"
	iface "github.com/ipfs/boxo/coreiface"
	"github.com/ipfs/boxo/coreiface/path"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/go-cid"
	"io"
)

type Store PoolClient

func (s *Store) Add(ctx context.Context, reader io.ReadCloser) (path.Resolved, error) {
	return s.api.Unixfs().Add(ctx, files.NewReaderFile(reader))
}
func (s *Store) Get(ctx context.Context, cidStr string) (io.ReadCloser, error) {
	meatCid, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}
	f, err := s.api.Unixfs().Get(ctx, path.IpfsPath(meatCid))
	if err != nil {
		return nil, err
	}
	var file files.File
	switch f := f.(type) {
	case files.File:
		file = f
	case files.Directory:
		return nil, iface.ErrIsDir
	default:
		return nil, iface.ErrNotSupported
	}
	return file, nil
}
