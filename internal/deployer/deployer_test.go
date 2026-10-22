package deployer

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nlewo/comin/internal/broker"
	"github.com/nlewo/comin/internal/store"
	"github.com/nlewo/comin/pkg/protobuf"
	"github.com/stretchr/testify/assert"
)

func TestDeployerBasic(t *testing.T) {
	deployDone := make(chan struct{})
	var deployFunc = func(context.Context, string, string, []string, io.WriteCloser, io.WriteCloser) (bool, string, error) {
		<-deployDone
		return false, "profile-path", nil
	}

	tmp := t.TempDir()
	bk := broker.New()
	bk.Start()

	s, err := store.New(bk, tmp+"/state.json", tmp+"/gcroots", 1, 1, 1)
	assert.Nil(t, err)
	d := New(s, deployFunc, nil, "", bk, 2, 100*time.Millisecond)
	d.Run(t.Context())
	assert.False(t, d.IsDeploying())

	g := &protobuf.Generation{
		Source: &protobuf.Source{
			Source: &protobuf.Source_Git{
				Git: &protobuf.Git{
					SelectedCommitId: "commit-1",
				},
			},
		},
	}
	d.Submit(g, "test", false, "")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, d.IsDeploying())
	}, 5*time.Second, 100*time.Millisecond)

	deployDone <- struct{}{}
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.False(c, d.IsDeploying())
		assert.Equal(c, "profile-path", d.Deployment().ProfilePath)
	}, 5*time.Second, 100*time.Millisecond)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		dpl := <-d.DeploymentDoneCh
		assert.Equal(c, "profile-path", dpl.ProfilePath)
		assert.Equal(c, "commit-1", dpl.Generation.Source.GetGit().SelectedCommitId)
	}, 5*time.Second, 100*time.Millisecond)
}

func TestDeployerSubmit(t *testing.T) {
	deployDone := make(chan struct{})
	var deployFunc = func(context.Context, string, string, []string, io.WriteCloser, io.WriteCloser) (bool, string, error) {
		<-deployDone
		return false, "profile-path", nil
	}

	tmp := t.TempDir()
	bk := broker.New()
	bk.Start()

	s, err := store.New(bk, tmp+"/state.json", tmp+"/gcroots", 1, 1, 1)
	assert.Nil(t, err)
	d := New(s, deployFunc, nil, "", bk, 2, 100*time.Millisecond)
	d.Run(t.Context())
	assert.False(t, d.IsDeploying())

	d.Submit(&protobuf.Generation{
		Source: &protobuf.Source{
			Source: &protobuf.Source_Git{
				Git: &protobuf.Git{
					SelectedCommitId: "commit-1",
				},
			},
		},
	}, "test", false, "")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(c, d.IsDeploying())
		assert.Nil(c, d.GenerationToDeploy)
	}, 5*time.Second, 100*time.Millisecond)

	d.Submit(&protobuf.Generation{
		Source: &protobuf.Source{
			Source: &protobuf.Source_Git{
				Git: &protobuf.Git{
					SelectedCommitId: "commit-2",
				},
			},
		},
	}, "test", false, "")
	d.Submit(&protobuf.Generation{
		Source: &protobuf.Source{
			Source: &protobuf.Source_Git{
				Git: &protobuf.Git{
					SelectedCommitId: "commit-3",
				},
			},
		},
	}, "test", false, "")
	assert.NotNil(t, d.GenerationToDeploy)

	// To simulate the end of 2 deployments (commit-1 and commit-3)
	deployDone <- struct{}{}
	deployDone <- struct{}{}
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.False(c, d.IsDeploying())
		assert.Equal(c, "profile-path", d.Deployment().ProfilePath)
		assert.Nil(t, d.GenerationToDeploy)
	}, 5*time.Second, 100*time.Millisecond)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		dpl := <-d.DeploymentDoneCh
		assert.Equal(c, "profile-path", dpl.ProfilePath)
		assert.Equal(c, "commit-1", dpl.Generation.Source.GetGit().SelectedCommitId)
	}, 5*time.Second, 100*time.Millisecond)
}

func TestDeployerSuspend(t *testing.T) {
	deployDone := make(chan struct{})
	var deployFunc = func(context.Context, string, string, []string, io.WriteCloser, io.WriteCloser) (bool, string, error) {
		<-deployDone
		return false, "profile-path", nil
	}

	tmp := t.TempDir()
	bk := broker.New()
	bk.Start()

	s, err := store.New(bk, tmp+"/state.json", tmp+"/gcroots", 1, 1, 1)
	assert.Nil(t, err)
	d := New(s, deployFunc, nil, "", bk, 2, 100*time.Millisecond)
	d.Run(t.Context())
	assert.False(t, d.IsSuspended())
	d.Suspend("suspended for testing")
	assert.True(t, d.IsSuspended())
	assert.False(t, d.IsDeploying())
	assert.False(t, d.RunnerIsSuspended())

	d.Submit(&protobuf.Generation{
		Source: &protobuf.Source{
			Source: &protobuf.Source_Git{
				Git: &protobuf.Git{
					SelectedCommitId: "commit-1",
				},
			},
		},
	}, "test", false, "")
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.True(t, d.RunnerIsSuspended())
	}, 3*time.Second, 100*time.Millisecond)

	d.Resume()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.False(t, d.RunnerIsSuspended())
		assert.True(t, d.IsDeploying())
	}, 3*time.Second, 100*time.Millisecond)
}

func TestDeployerRetryOnFailure(t *testing.T) {
	var attempts atomic.Int32
	var deployFunc = func(context.Context, string, string, []string, io.WriteCloser, io.WriteCloser) (bool, string, error) {
		// Fail the first attempt, succeed on the second one
		if attempts.Add(1) == 1 {
			return false, "", fmt.Errorf("deployment failed")
		}
		return false, "profile-path", nil
	}

	tmp := t.TempDir()
	bk := broker.New()
	bk.Start()

	s, err := store.New(bk, tmp+"/state.json", tmp+"/gcroots", 1, 1, 1)
	assert.Nil(t, err)
	// 2 attempts (1 initial + 1 retry)
	d := New(s, deployFunc, nil, "", bk, 2, 50*time.Millisecond)
	d.Run(t.Context())

	d.Submit(&protobuf.Generation{
		Source: &protobuf.Source{
			Source: &protobuf.Source_Git{
				Git: &protobuf.Git{
					SelectedCommitId: "commit-1",
				},
			},
		},
	}, "test", false, "")

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int32(2), attempts.Load(), "deployment should be attempted twice")
	}, 5*time.Second, 100*time.Millisecond)

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		dpl := <-d.DeploymentDoneCh
		assert.Equal(c, store.StatusToString(store.Done), dpl.Status)
		assert.Equal(c, "profile-path", dpl.ProfilePath)
		assert.Equal(c, "", dpl.ErrorMsg)
	}, 5*time.Second, 100*time.Millisecond)
}

func TestDeployerRetryRespectsAttemptsLimit(t *testing.T) {
	var attempts atomic.Int32
	var deployFunc = func(context.Context, string, string, []string, io.WriteCloser, io.WriteCloser) (bool, string, error) {
		attempts.Add(1)
		return false, "", fmt.Errorf("deployment failed")
	}

	tmp := t.TempDir()
	bk := broker.New()
	bk.Start()

	s, err := store.New(bk, tmp+"/state.json", tmp+"/gcroots", 1, 1, 1)
	assert.Nil(t, err)
	// 3 attempts (1 initial + 2 retries)
	d := New(s, deployFunc, nil, "", bk, 3, 50*time.Millisecond)
	d.Run(t.Context())

	d.Submit(&protobuf.Generation{
		Source: &protobuf.Source{
			Source: &protobuf.Source_Git{
				Git: &protobuf.Git{
					SelectedCommitId: "commit-1",
				},
			},
		},
	}, "test", false, "")

	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		dpl := <-d.DeploymentDoneCh
		assert.Equal(c, store.StatusToString(store.Failed), dpl.Status)
		assert.Equal(c, "deployment failed", dpl.ErrorMsg)
	}, 5*time.Second, 100*time.Millisecond)

	// No additional attempt should have been performed
	assert.Equal(t, int32(3), attempts.Load(), "deployment should be attempted exactly 3 times")
}
