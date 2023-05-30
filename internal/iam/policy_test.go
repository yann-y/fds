package iam

import (
	"context"
	"fmt"
	"github.com/yann-y/fds/internal/iam/auth"
	"github.com/yann-y/fds/internal/iam/policy"
	"github.com/yann-y/fds/internal/iam/policy/condition"
	"github.com/yann-y/fds/internal/iam/s3action"
	"github.com/yann-y/fds/internal/uleveldb"
	"testing"
)

func TestPolicySys_IsAllowed(t *testing.T) {
	db, _ := uleveldb.OpenDb(t.TempDir())
	iamSys := NewIdentityAMSys(db)
	//poli := NewIPolicySys(db)
	initSys()
	if iamSys.IsAllowed(context.Background(), auth.Args{
		AccountName: auth.DefaultAccessKey,
		Action:      "list",
		BucketName:  "test",
		ObjectName:  "test",
		IsOwner:     false,
	}) {
		// Request is allowed return the appropriate access key.
		fmt.Println(true)
	}
}

func initSys() {
	var states []policy.Statement

	ast := s3action.NewActionSet("list")
	principal := policy.NewPrincipal(auth.DefaultAccessKey)
	resource := policy.NewResourceSet()
	states = append(states, policy.NewStatement("1", policy.Allow, principal, ast, resource, condition.NewConFunctions()))

}
