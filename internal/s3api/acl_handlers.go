package s3api

import (
	"github.com/yann-y/fds/internal/apierrors"
	"github.com/yann-y/fds/internal/consts"
	"github.com/yann-y/fds/internal/iam/policy"
	"github.com/yann-y/fds/internal/iam/s3action"
	"github.com/yann-y/fds/internal/response"
	"github.com/yann-y/fds/internal/utils"
	"github.com/yann-y/fds/pkg/s3utils"
	"io"
	"net/http"
)

func checkPermissionType(s string) bool {
	switch s {
	case policy.PublicRead:
		return true
	case policy.PublicReadWrite:
		return true
	case policy.Private:
		return true
	case policy.Default:
		return true
	}
	return false
}

// 上传对象时设置object ACL，目前只支持private | public-read | public-read-write | default
// 如果传其他的，默认default
func checkPutObjectACL(acl string) string {
	switch acl {
	case policy.PublicRead:
		return acl
	case policy.PublicReadWrite:
		return acl
	case policy.Private:
		return acl
	default:
		return policy.Default
	}
}

// GetBucketAclHandler Get Bucket ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketAcl.html
func (s3a *s3ApiServer) GetBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	// collect parameters
	bucket, _, _ := getBucketAndObject(r)
	log.Infof("GetBucketAclHandler %s", bucket)
	cred, _, s3err := s3a.authSys.CheckRequestAuthTypeCredential(r.Context(), r, s3action.GetBucketPolicyAction, bucket, "")
	if s3err != apierrors.ErrNone {
		response.WriteErrorResponse(w, r, s3err)
		return
	}
	ctx := r.Context()
	if !s3a.bmSys.HasBucket(ctx, bucket) {
		response.WriteErrorResponseHeadersOnly(w, r, apierrors.ErrNoSuchBucket)
		return
	}
	acl, err := s3a.bmSys.GetBucketAcl(ctx, bucket)
	if err != nil {
		response.WriteErrorResponse(w, r, apierrors.ToApiError(ctx, err))
		return
	}
	// 校验桶ACL类型，公共读(PublicRead)，公共读写(PublicReadWrite)，私有(Private)
	if acl == "" {
		acl = "private"
	}
	resp := response.AccessControlPolicy{}
	id := cred.AccessKey
	if resp.Owner.DisplayName == "" {
		resp.Owner.DisplayName = cred.AccessKey
		resp.Owner.ID = id
	}
	resp.AccessControlList.Grant = append(resp.AccessControlList.Grant, response.Grant{
		Grantee: response.Grantee{
			ID:          id,
			DisplayName: cred.AccessKey,
			Type:        "CanonicalUser",
			XMLXSI:      "CanonicalUser",
			XMLNS:       "http://www.w3.org/2001/XMLSchema-instance"},
		Permission: response.Permission(acl), //todo change
	})
	response.WriteSuccessResponseXML(w, r, resp)
}

// PutBucketAclHandler Put bucket ACL
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketAcl.html
func (s3a *s3ApiServer) PutBucketAclHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _, _ := getBucketAndObject(r)

	// Allow putBucketACL if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	cred, _, s3err := s3a.authSys.CheckRequestAuthTypeCredential(r.Context(), r, s3action.PutBucketPolicyAction, bucket, "")
	if s3err != apierrors.ErrNone {
		response.WriteErrorResponse(w, r, s3err)
		return
	}

	aclHeader := r.Header.Get(consts.AmzACL)
	if aclHeader == "" {
		acl := &response.AccessControlPolicy{}
		if errc := utils.XmlDecoder(r.Body, acl, r.ContentLength); errc != nil {
			if errc == io.EOF {
				response.WriteErrorResponse(w, r, apierrors.ErrMissingSecurityHeader)
				return
			}
			response.WriteErrorResponse(w, r, apierrors.ErrInternalError)
			return
		}

		if len(acl.AccessControlList.Grant) == 0 {
			response.WriteErrorResponse(w, r, apierrors.ErrNotImplemented)
			return
		}

		if acl.AccessControlList.Grant[0].Permission != "FULL_CONTROL" {
			response.WriteErrorResponse(w, r, apierrors.ErrNotImplemented)
			return
		}
	}

	if !checkPermissionType(aclHeader) {
		response.WriteErrorResponse(w, r, apierrors.ErrNotImplemented)
		return
	}
	ctx := r.Context()
	err := s3a.bmSys.UpdateBucketAcl(ctx, bucket, aclHeader, cred.AccessKey)
	if err != nil {
		response.WriteErrorResponse(w, r, apierrors.ToApiError(ctx, err))
		return
	}
}

// object ACL：包括private（私有）、public-read（公开读）、public-read-write（公开读写）、default（默认），
// 支持创建（PUT）、更新（PUT）、查询（GET）、删除(DELETE)。
// private（私有）:该ACL表明某个Object是私有资源，即只有该Object的Owner拥有该Object的读写权限，其他的用户没有权限操作该Object。
// public-read（公开读）:	该ACL表明某个Object是公共读资源，即非Object Owner只有该Object的读权限，而Object Owner拥有该Object的读写权限。
// public-read-write（公开读写）:该ACL表明某个Object是公共读写资源，即所有用户拥有对该Object的读写权限。
// default（默认）:该ACL表明某个Object是遵循Bucket读写权限的资源，即Bucket是什么权限，Object就是什么权限。

// PutObjectAclHandler - PUT Object ACL
// -----------------
// This operation uses the ACL subresource
// to set ACL for a bucket, this is a dummy call
// only responds success if the ACL is private.
func (s3a *s3ApiServer) PutObjectAclHandler(w http.ResponseWriter, r *http.Request) {
	bucket, object, _ := getBucketAndObject(r)

	// Allow putBucketACL if policy action is set, since this is a dummy call
	// we are simply re-purposing the bucketPolicyAction.
	_, _, s3err := s3a.authSys.CheckRequestAuthTypeCredential(r.Context(), r, s3action.PutBucketPolicyAction, bucket, "")
	if s3err != apierrors.ErrNone {
		response.WriteErrorResponse(w, r, s3err)
		return
	}

	aclHeader := r.Header.Get(consts.AmzACL)
	if aclHeader == "" {
		acl := &response.AccessControlPolicy{}
		if errc := utils.XmlDecoder(r.Body, acl, r.ContentLength); errc != nil {
			if errc == io.EOF {
				response.WriteErrorResponse(w, r, apierrors.ErrMissingSecurityHeader)
				return
			}
			response.WriteErrorResponse(w, r, apierrors.ErrInternalError)
			return
		}

		if len(acl.AccessControlList.Grant) == 0 {
			response.WriteErrorResponse(w, r, apierrors.ErrNotImplemented)
			return
		}

		if acl.AccessControlList.Grant[0].Permission != "FULL_CONTROL" {
			response.WriteErrorResponse(w, r, apierrors.ErrNotImplemented)
			return
		}
	}

	if !checkPermissionType(aclHeader) {
		response.WriteErrorResponse(w, r, apierrors.ErrNotImplemented)
		return
	}
	ctx := r.Context()
	objectInfo, err := s3a.store.GetObjectInfo(ctx, bucket, object)
	if err != nil {
		response.WriteErrorResponse(w, r, apierrors.ToApiError(ctx, err))
		return
	}
	objectInfo.Acl = aclHeader
	err = s3a.store.PutObjectInfo(ctx, objectInfo)
	if err != nil {
		response.WriteErrorResponse(w, r, apierrors.ToApiError(ctx, err))
		return
	}
}

// GetObjectACLHandler - GET Object ACL
// -----------------
// This operation uses the ACL
// subresource to return the ACL of a specified object.
func (s3a *s3ApiServer) GetObjectACLHandler(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()
	bucket, object, err := getBucketAndObject(r)
	if err != nil {
		response.WriteErrorResponseHeadersOnly(w, r, apierrors.ToApiError(ctx, err))
		return
	}
	log.Infof("HeadObjectHandler %s %s", bucket, object)
	if err := s3utils.CheckGetObjArgs(ctx, bucket, object); err != nil {
		response.WriteErrorResponseHeadersOnly(w, r, apierrors.ToApiError(ctx, err))
		return
	}

	// Check for auth type to return S3 compatible error.
	// type to return the correct error (NoSuchKey vs AccessDenied)
	cred, _, s3Error := s3a.authSys.CheckRequestAuthTypeCredential(ctx, r, s3action.GetBucketPolicyAction, bucket, object)
	if s3Error != apierrors.ErrNone {
		response.WriteErrorResponseHeadersOnly(w, r, s3Error)
		return
	}
	if !s3a.bmSys.HasBucket(ctx, bucket) {
		response.WriteErrorResponseHeadersOnly(w, r, apierrors.ErrNoSuchBucket)
		return
	}
	objInfo, err := s3a.store.GetObjectInfo(ctx, bucket, object)
	if err != nil {
		response.WriteErrorResponseHeadersOnly(w, r, apierrors.ToApiError(ctx, err))
		return
	}
	acl := checkPutObjectACL(objInfo.Acl)
	resp := response.AccessControlPolicy{}
	id := cred.AccessKey
	if resp.Owner.DisplayName == "" {
		resp.Owner.DisplayName = cred.AccessKey
		resp.Owner.ID = id
	}
	resp.AccessControlList.Grant = append(resp.AccessControlList.Grant, response.Grant{
		Grantee: response.Grantee{
			ID:          id,
			DisplayName: cred.AccessKey,
			Type:        "CanonicalUser",
			XMLXSI:      "CanonicalUser",
			XMLNS:       "http://www.w3.org/2001/XMLSchema-instance"},
		Permission: response.Permission(acl), //todo change
	})
	response.WriteSuccessResponseXML(w, r, resp)
}
