package store

import (
	"context"
	"github.com/yann-y/fds/internal/iam/policy"
)

func (sys *BucketMetadataSys) UpdateBucketAcl(ctx context.Context, bucket, acl, accessKey string) error {
	lk := sys.NewNSLock(bucket)
	lkctx, err := lk.GetLock(ctx, globalOperationTimeout)
	if err != nil {
		return err
	}
	ctx = lkctx.Context()
	defer lk.Unlock(lkctx.Cancel)

	meta, err := sys.getBucketMeta(bucket)
	if err != nil {
		return err
	}

	meta.Acl = acl
	newPolicy := policy.CreateBucketPolicy(bucket, accessKey, acl)
	meta.PolicyConfig = newPolicy
	return sys.setBucketMeta(bucket, &meta)
}
func (sys *BucketMetadataSys) GetBucketAcl(ctx context.Context, bucket string) (string, error) {
	meta, err := sys.GetBucketMeta(ctx, bucket)
	if err != nil {
		switch err.(type) {
		case BucketNotFound:
			return "", BucketTaggingNotFound{Bucket: bucket}
		}
		return "", err
	}
	return meta.Acl, nil
}
