package store

import (
	"context"
	"fmt"
	"github.com/yann-y/fds/internal/iam/policy"
	"github.com/yann-y/fds/internal/iam/policy/condition"
	"github.com/yann-y/fds/internal/iam/s3action"
	"github.com/yann-y/fds/internal/uleveldb"
	"testing"
	"time"
)

func TestBucketMetadataSys_BucketMetadata(t *testing.T) {
	db, err := uleveldb.OpenDb(t.TempDir())
	if err != nil {
		return
	}
	s := NewBucketMetadataSys(db)
	s.SetEmptyBucket(func(ctx context.Context, bucket string) (bool, error) {
		return true, nil
	})
	err = s.setBucketMeta("bucket", &BucketMetadata{
		Name:          "bucket",
		Region:        "region",
		Created:       time.Now(),
		PolicyConfig:  &policy.Policy{},
		TaggingConfig: &Tags{},
	})
	if err != nil {
		return
	}
	meta, err := s.GetBucketMeta(context.TODO(), "bucket")
	if err != nil {
		return
	}
	fmt.Println(meta)
	err = s.DeleteBucket(context.TODO(), "bucket")
	if err != nil {
		return
	}
	ok := s.HasBucket(context.TODO(), "bucket")

	fmt.Println(ok)
}
func TestBucketMetadataSys_GetPolicyConfig(t *testing.T) {
	db, err := uleveldb.OpenDb(t.TempDir())
	if err != nil {
		return
	}
	s := NewBucketMetadataSys(db)
	c, _ := condition.NewStringEqualsFunc("", condition.S3Prefix.ToKey(), "object.txt")
	err = s.setBucketMeta("bucket", &BucketMetadata{
		Name:    "bucket",
		Region:  "region",
		Created: time.Now(),
		PolicyConfig: &policy.Policy{
			ID:      "id",
			Version: "1",
			Statements: []policy.Statement{
				{
					Effect:     "Allow",
					Principal:  policy.NewPrincipal("accessKey"),
					Actions:    s3action.SupportedActions,
					Resources:  policy.NewResourceSet(policy.NewResource("bucket", "*")),
					Conditions: condition.NewConFunctions(c),
				},
			},
		},
		TaggingConfig: &Tags{},
	})
	if err != nil {
		return
	}
	p, err := s.GetPolicyConfig(context.TODO(), "bucket")
	if err != nil {
		return
	}
	fmt.Println(p)
}
