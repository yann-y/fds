package ipfs

import (
	"github.com/ipfs/go-cid"
	chunker "github.com/ipfs/go-ipfs-chunker"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	ft "github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/importer/balanced"
	h "github.com/ipfs/go-unixfs/importer/helpers"
	"io"
)

const unixfsLinksPerLevel = 1 << 10
const unixfsChunkSize uint64 = 1 << 18

// BalanceNode split the file and store it in DAGService as node
func BalanceNode(f io.Reader, bufDs ipld.DAGService, cidBuilder cid.Builder) (node ipld.Node, err error) {
	params := h.DagBuilderParams{
		Maxlinks:   h.DefaultLinksPerBlock,
		RawLeaves:  false,
		CidBuilder: cidBuilder,
		Dagserv:    bufDs,
		NoCopy:     false,
	}
	db, err := params.New(chunker.NewSizeSplitter(f, int64(unixfsChunkSize)))
	if err != nil {
		return nil, err
	}
	node, err = balanced.Layout(db)
	if err != nil {
		return nil, err
	}
	return
}

type LinkInfo struct {
	Link     *ipld.Link
	FileSize uint64
}

type unixfsNode struct {
	dag  *dag.ProtoNode
	file *ft.FSNode
}

func NewUnixfsNodeFromDag(nd *dag.ProtoNode) (*unixfsNode, error) {
	mb, err := ft.FSNodeFromBytes(nd.Data())
	if err != nil {
		return nil, err
	}

	return &unixfsNode{
		dag:  nd,
		file: mb,
	}, nil
}

func (n *unixfsNode) AddChild(child *ipld.Link, fileSize uint64) error {
	err := n.dag.AddRawLink("", child)
	if err != nil {
		return err
	}

	n.file.AddBlockSize(fileSize)

	return nil
}

func (n *unixfsNode) Commit() (ipld.Node, error) {
	fileData, err := n.file.GetBytes()
	if err != nil {
		return nil, err
	}
	n.dag.SetData(fileData)

	return n.dag, nil
}
