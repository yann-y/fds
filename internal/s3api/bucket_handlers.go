package s3api

import (
	"encoding/xml"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	logging "github.com/ipfs/go-log/v2"
	"github.com/yann-y/fds/internal/apierrors"
	"github.com/yann-y/fds/internal/consts"
	"github.com/yann-y/fds/internal/iam/policy"
	"github.com/yann-y/fds/internal/iam/s3action"
	"github.com/yann-y/fds/internal/response"
	"github.com/yann-y/fds/internal/store"
	"github.com/yann-y/fds/internal/utils"
	"github.com/yann-y/fds/pkg/s3utils"
	"io"
	"net/http"
	"path"
)

var log = logging.Logger("server")

// ListBucketsHandler ListBuckets Handler
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
func (s3a *s3ApiServer) ListBucketsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	cred, _, s3err := s3a.authSys.CheckRequestAuthTypeCredential(ctx, r, s3action.ListAllMyBucketsAction, "", "")
	if s3err != apierrors.ErrNone {
		response.WriteErrorResponse(w, r, s3err)
		return
	}
	log.Info("ListBucketsHandler")
	// Anonymous users, should be rejected.
	if cred.AccessKey == "" {
		response.WriteErrorResponse(w, r, apierrors.ErrAccessDenied)
		return
	}
	bucketMetas, err := s3a.bmSys.GetAllBucketsOfUser(ctx, cred.AccessKey)
	if err != nil {
		response.WriteErrorResponse(w, r, apierrors.ToApiError(ctx, err))
		return
	}
	var buckets []*s3.Bucket
	for _, b := range bucketMetas {
		buckets = append(buckets, &s3.Bucket{
			Name:         aws.String(b.Name),
			CreationDate: aws.Time(b.Created),
		})
	}

	resp := response.ListAllMyBucketsResult{
		Owner: &s3.Owner{
			ID:          aws.String(consts.DefaultOwnerID),
			DisplayName: aws.String(consts.DisplayName),
		},
		Buckets: buckets,
	}

	response.WriteSuccessResponseXML(w, r, resp)
}

// GetBucketLocationHandler - GET Bucket location.
// -------------------------
// This operation returns bucket location.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketLocation.html
func (s3a *s3ApiServer) GetBucketLocationHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _, _ := getBucketAndObject(r)
	ctx := r.Context()
	_, _, s3err := s3a.authSys.CheckRequestAuthTypeCredential(ctx, r, s3action.ListAllMyBucketsAction, bucket, "")
	if s3err != apierrors.ErrNone {
		response.WriteErrorResponse(w, r, s3err)
		return
	}
	bucketMetas, err := s3a.bmSys.GetBucketMeta(ctx, bucket)
	if err != nil {
		response.WriteErrorResponse(w, r, apierrors.ToApiError(ctx, err))
		return
	}

	// Generate response.
	encodedSuccessResponse := response.LocationResponse{
		Location: bucketMetas.Region,
	}

	// Write success response.
	response.WriteSuccessResponseXML(w, r, encodedSuccessResponse)
}

// PutBucketHandler put a bucket
func (s3a *s3ApiServer) PutBucketHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _, _ := getBucketAndObject(r)
	ctx := r.Context()
	log.Infof("PutBucketHandler %s", bucket)
	region, _ := parseLocationConstraint(r)
	// avoid duplicated buckets
	cred, _, s3err := s3a.authSys.CheckRequestAuthTypeCredential(ctx, r, s3action.CreateBucketAction, bucket, "")
	if s3err != apierrors.ErrNone {
		response.WriteErrorResponse(w, r, s3err)
		return
	}

	if err := s3utils.CheckValidBucketNameStrict(bucket); err != nil {
		response.WriteErrorResponse(w, r, apierrors.ErrInvalidBucketName)
		return
	}
	aclHeader := r.Header.Get(consts.AmzACL)
	if !checkPermissionType(aclHeader) {
		aclHeader = policy.Private
	}
	err := s3a.bmSys.CreateBucket(ctx, bucket, region, cred.AccessKey, aclHeader)
	if err != nil {
		log.Errorf("PutBucketHandler create bucket error:%v", s3err)
		response.WriteErrorResponse(w, r, apierrors.ToApiError(ctx, err))
		return
	}

	// Make sure to add Location information here only for bucket
	if cp := pathClean(r.URL.Path); cp != "" {
		w.Header().Set(consts.Location, cp) // Clean any trailing slashes.
	}

	response.WriteSuccessResponseHeadersOnly(w, r)
}

// HeadBucketHandler - HEAD Bucket
// ----------
// This operation is useful to determine if a bucket exists.
// The operation returns a 200 OK if the bucket exists and you
// have permission to access it. Otherwise, the operation might
// return responses such as 404 Not Found and 403 Forbidden.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
func (s3a *s3ApiServer) HeadBucketHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _, _ := getBucketAndObject(r)
	log.Infof("HeadBucketHandler %s", bucket)
	// avoid duplicated buckets
	_, _, s3err := s3a.authSys.CheckRequestAuthTypeCredential(r.Context(), r, s3action.HeadBucketAction, bucket, "")
	if s3err != apierrors.ErrNone {
		response.WriteErrorResponseHeadersOnly(w, r, s3err)
		return
	}

	if ok := s3a.bmSys.HasBucket(r.Context(), bucket); !ok {
		response.WriteErrorResponseHeadersOnly(w, r, apierrors.ErrNoSuchBucket)
		return
	}

	response.WriteSuccessResponseHeadersOnly(w, r)
}

// DeleteBucketHandler delete Bucket
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html
func (s3a *s3ApiServer) DeleteBucketHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _, _ := getBucketAndObject(r)
	ctx := r.Context()
	log.Infof("DeleteBucketHandler %s", bucket)
	_, _, s3err := s3a.authSys.CheckRequestAuthTypeCredential(ctx, r, s3action.DeleteBucketAction, bucket, "")
	if s3err != apierrors.ErrNone {
		response.WriteErrorResponse(w, r, s3err)
		return
	}

	err := s3a.bmSys.DeleteBucket(ctx, bucket)
	if err != nil {
		response.WriteErrorResponse(w, r, apierrors.ToApiError(ctx, err))
		return
	}
	response.WriteSuccessNoContent(w)
}

// GetBucketCorsHandler Get bucket CORS
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketCors.html
func (s3a *s3ApiServer) GetBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	response.WriteErrorResponse(w, r, apierrors.ErrNoSuchCORSConfiguration)
}

// PutBucketCorsHandler Put bucket CORS
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketCors.html
func (s3a *s3ApiServer) PutBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	response.WriteErrorResponse(w, r, apierrors.ErrNotImplemented)
}

// DeleteBucketCorsHandler Delete bucket CORS
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketCors.html
func (s3a *s3ApiServer) DeleteBucketCorsHandler(w http.ResponseWriter, r *http.Request) {
	response.WriteErrorResponse(w, r, http.StatusNoContent)
}

// PutBucketTaggingHandler
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketTagging.html
func (s3a *s3ApiServer) PutBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _, _ := getBucketAndObject(r)
	ctx := r.Context()
	log.Infof("DeleteBucketHandler %s", bucket)
	_, _, s3err := s3a.authSys.CheckRequestAuthTypeCredential(ctx, r, s3action.DeleteBucketAction, bucket, "")
	if s3err != apierrors.ErrNone {
		response.WriteErrorResponse(w, r, s3err)
		return
	}

	tags, err := unmarshalXML(io.LimitReader(r.Body, r.ContentLength), false)
	if err != nil {
		response.WriteErrorResponse(w, r, apierrors.ErrMalformedXML)
		return
	}

	if err = s3a.bmSys.UpdateBucketTagging(ctx, bucket, tags); err != nil {
		response.WriteErrorResponse(w, r, apierrors.ToApiError(ctx, err))
		return
	}

	// Write success response.
	response.WriteSuccessResponseHeadersOnly(w, r)
}

// GetBucketTaggingHandler
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketTagging.html
func (s3a *s3ApiServer) GetBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _, _ := getBucketAndObject(r)
	ctx := r.Context()
	log.Infof("DeleteBucketHandler %s", bucket)
	_, _, s3err := s3a.authSys.CheckRequestAuthTypeCredential(ctx, r, s3action.DeleteBucketAction, bucket, "")
	if s3err != apierrors.ErrNone {
		response.WriteErrorResponse(w, r, s3err)
		return
	}

	tags, err := s3a.bmSys.GetTaggingConfig(ctx, bucket)
	if err != nil {
		response.WriteErrorResponse(w, r, apierrors.ToApiError(ctx, err))
		return
	}
	configData, err2 := xml.Marshal(tags)
	if err2 != nil {
		response.WriteErrorResponse(w, r, apierrors.ErrMalformedXML)
		return
	}

	// Write success response.
	response.WriteSuccessResponseXML(w, r, configData)
}

// DeleteBucketTaggingHandler
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucketTagging.html
func (s3a *s3ApiServer) DeleteBucketTaggingHandler(w http.ResponseWriter, r *http.Request) {
	bucket, _, _ := getBucketAndObject(r)
	ctx := r.Context()
	log.Infof("DeleteBucketHandler %s", bucket)
	_, _, s3err := s3a.authSys.CheckRequestAuthTypeCredential(ctx, r, s3action.DeleteBucketAction, bucket, "")
	if s3err != apierrors.ErrNone {
		response.WriteErrorResponse(w, r, s3err)
		return
	}

	if err := s3a.bmSys.DeleteBucketTagging(ctx, bucket); err != nil {
		response.WriteErrorResponse(w, r, apierrors.ToApiError(ctx, err))
		return
	}

	// Write success response.
	response.WriteSuccessResponseHeadersOnly(w, r)
}

// Parses location constraint from the incoming reader.
func parseLocationConstraint(r *http.Request) (location string, s3Error apierrors.ErrorCode) {
	// If the request has no body with content-length set to 0,
	// we do not have to validate location constraint. Bucket will
	// be created at default region.
	locationConstraint := createBucketLocationConfiguration{}
	err := utils.XmlDecoder(r.Body, &locationConstraint, r.ContentLength)
	if err != nil && r.ContentLength != 0 {
		// Treat all other failures as XML parsing errors.
		return "", apierrors.ErrMalformedXML
	} // else for both err as nil or io.EOF
	location = locationConstraint.Location
	if location == "" {
		location = consts.DefaultRegion
	}
	return location, apierrors.ErrNone
}

// createBucketConfiguration container for bucket configuration request from client.
// Used for parsing the location from the request body for Makebucket.
type createBucketLocationConfiguration struct {
	XMLName  xml.Name `xml:"CreateBucketConfiguration" json:"-"`
	Location string   `xml:"LocationConstraint"`
}

// pathClean is like path.Clean but does not return "." for
// empty inputs, instead returns "empty" as is.
func pathClean(p string) string {
	cp := path.Clean(p)
	if cp == "." {
		return ""
	}
	return cp
}

func unmarshalXML(reader io.Reader, isObject bool) (*store.Tags, error) {
	tagging := &store.Tags{
		TagSet: &store.TagSet{
			TagMap:   make(map[string]string),
			IsObject: isObject,
		},
	}

	if err := xml.NewDecoder(reader).Decode(tagging); err != nil {
		return nil, err
	}

	return tagging, nil
}
