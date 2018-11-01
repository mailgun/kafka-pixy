package kazoo

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
)

const (
	versionAny = -1
)

// Model represent Kafka consumer group data model stored in ZooKeeper. It
// provides high level functions to deal with group member subscriptions and
// topic partition ownership. A model is bound to a particular member of a
// particular consumer group.
type Model struct {
	zkConn      *zk.Conn
	log         *logrus.Entry
	groupPath   string
	membersPath string
	ownersPath  string
	memberID    string
	memberPath  string
}

// NewModel creates a model instance bound to a member of a consumer group.
func NewModel(zkConn *zk.Conn, chroot, group, memberID string, log *logrus.Entry) Model {
	groupPath := fmt.Sprintf("%s/consumers/%s", chroot, group)
	membersPath := groupPath + "/ids"
	return Model{
		zkConn:      zkConn,
		log:         log,
		groupPath:   groupPath,
		membersPath: membersPath,
		ownersPath:  groupPath + "/owners",
		memberID:    memberID,
		memberPath:  membersPath + "/" + memberID,
	}
}

// EnsureMemberSubscription creates, updates or even deletes a member
// specification znode to ensure that the bound member is subscribed to the
// given topics.
func (m *Model) EnsureMemberSubscription(topics []string) error {
	if len(topics) == 0 {
		if err := m.zkConn.Delete(m.memberPath, versionAny); err != nil {
			if err == zk.ErrNoNode {
				return nil
			}
			return errors.Wrapf(err, "while deleting %v", m.memberPath)
		}
		return nil
	}

	memberSpec := newMemberSpec(topics)
	memberSpecJSON, err := json.Marshal(memberSpec)
	if err != nil {
		return errors.Wrapf(err, "while JSON encoding %s", spew.Sdump(memberSpec))
	}

	if err := m.durableUpsertZNode(m.memberPath, memberSpecJSON, zk.FlagEphemeral); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// FetchGroupSubscriptions retrieves bound group member specification records
// and returns memberID-to-topic-list map, along with a channel that will be
// sent a message when either the number of members or subscription of any of
// them changes.
func (m *Model) FetchGroupSubscriptions() (map[string][]string, <-chan none.T, context.CancelFunc, error) {
	members, memberWatchCh, err := m.watchZNodeChildren(m.membersPath)
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "failed to watch members")
	}

	memberUpdateWatchChs := make(map[string]<-chan zk.Event, len(members))
	subscriptions := make(map[string][]string, len(members))
	for _, memberID := range members {
		memberPath := m.memberZNodePath(memberID)
		jsonMemberSpec, _, memberUpdateWatchCh, err := m.zkConn.GetW(memberPath)
		if err == zk.ErrNoNode {
			continue
		}
		if err != nil {
			return nil, nil, nil, errors.Wrapf(err, "while getting znode %s", memberPath)
		}

		// Parse the retrieved JSON encoded member spec.
		var memberSpec memberSpec
		if err := json.Unmarshal(jsonMemberSpec, &memberSpec); err != nil {
			return nil, nil, nil, errors.Wrapf(err, "while parsing member %s, data=%s", memberID, string(jsonMemberSpec))
		}

		memberUpdateWatchChs[memberID] = memberUpdateWatchCh
		subscriptions[memberID] = memberSpec.topics()
	}
	aggregateWatchCh := make(chan none.T)
	ctx, cancel := context.WithCancel(context.Background())

	go m.forwardWatch(ctx, "members", memberWatchCh, aggregateWatchCh)
	for memberID, memberUpdateWatchCh := range memberUpdateWatchChs {
		go m.forwardWatch(ctx, memberID, memberUpdateWatchCh, aggregateWatchCh)
	}
	return subscriptions, aggregateWatchCh, cancel, nil
}

// CreatePartitionOwner creates a partition owner znode, but only if none
// exists for the given topic-partition. An error is returned if a partition
// owner znode exists but belongs to another member.
func (m *Model) CreatePartitionOwner(topic string, partition int32) error {
	path := m.partitionOwnerZNodePath(topic, partition)
	err := m.durableCreateZNode(path, []byte(m.memberID), zk.FlagEphemeral)
	if errors.Cause(err) == zk.ErrNodeExists {
		rawOwnerID, _, err := m.zkConn.Get(path)
		ownerID := string(rawOwnerID)
		if err != nil {
			return err
		}
		if ownerID != m.memberID {
			return errors.Errorf("owned by %s", ownerID)
		}
		return nil
	}
	if err != nil {
		return errors.Wrapf(err, "while deep creating %s", path)
	}
	return nil
}

// DeletePartitionOwner deletes a partition owner znode, but only if belongs
// to the bound member, an error is returned otherwise.
func (m *Model) DeletePartitionOwner(topic string, partition int32) error {
	path := m.partitionOwnerZNodePath(topic, partition)
	rawOwnerID, stat, err := m.zkConn.Get(path)
	if err == zk.ErrNoNode {
		return nil
	}
	if err != nil {
		return errors.Wrap(err, "while getting owner znode")
	}
	ownerID := string(rawOwnerID)
	if ownerID != m.memberID {
		m.log.Warnf("Could not release partition owned by %s", ownerID)
		return nil
	}
	if err := m.zkConn.Delete(path, stat.Version); err != nil {
		return errors.Wrap(err, "while deleting owner znode")
	}
	return nil
}

// DeleteGroupIfEmpty deletes group data structures in ZooKeeper, if it is empty.
func (m *Model) DeleteGroupIfEmpty() error {
	// Check if the group still has members.
	members, _, err := m.zkConn.Children(m.membersPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil
		}
		return errors.Wrapf(err, "while getting %v children", m.membersPath)
	}
	// If the group has members, then it should not be cleaned up.
	if len(members) > 0 {
		return nil
	}
	// Collect all group paths that need to be deleted. The orders has to be
	// from bottom up, so when a path is deleted it has no children.
	topics, _, err := m.zkConn.Children(m.ownersPath)
	if err != nil && err != zk.ErrNoNode {
		return errors.Wrapf(err, "while getting %v children", m.ownersPath)
	}
	// Sort topics to make tests deterministic.
	sort.Strings(topics)
	toDeletePaths := make([]string, 0, len(topics)+3)
	for _, topic := range topics {
		toDeletePaths = append(toDeletePaths, m.ownersPath+"/"+topic)
	}
	toDeletePaths = append(toDeletePaths, m.ownersPath, m.membersPath, m.groupPath)
	// Delete all group data structure paths.
	for _, path := range toDeletePaths {
		err = m.zkConn.Delete(path, versionAny)
		if err != nil && err != zk.ErrNoNode {
			return errors.Wrapf(err, "while deleting %v", path)
		}
	}
	return nil
}

func (m *Model) GetPartitionOwner(topic string, partition int32) (string, error) {
	path := m.partitionOwnerZNodePath(topic, partition)
	rawOwnerID, _, err := m.zkConn.Get(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return "", nil
		}
		return "", errors.WithStack(err)
	}
	return string(rawOwnerID), nil
}

func (m *Model) memberZNodePath(memberID string) string {
	return fmt.Sprintf("%s/%s", m.membersPath, memberID)
}

func (m *Model) partitionOwnerZNodePath(topic string, partition int32) string {
	return fmt.Sprintf("%s/%s/%d", m.ownersPath, topic, partition)
}

func (m *Model) forwardWatch(ctx context.Context, alias string, fromCh <-chan zk.Event, toCh chan<- none.T) {
	select {
	case <-fromCh:
		m.log.Infof("Watch triggered: alias=%s", alias)
		select {
		case toCh <- none.V:
		case <-ctx.Done():
		}
	case <-ctx.Done():
	}
}

// durableCreateZNode reliably creates a ZNode in presence of racing actors
// trying to delete ancestor directories of its path. It never fails with cause
// zk.ErrNoNode. Returns an error with cause zk.ErrNodeExists if such ZNode
// exists.
func (m *Model) durableCreateZNode(path string, val []byte, flags int32) error {
	for {
		_, err := m.zkConn.Create(path, val, flags, zk.WorldACL(zk.PermAll))
		if err != nil {
			if err != zk.ErrNoNode {
				return errors.Wrapf(err, "while creating %v", path)
			}
			err := m.durableCreateParentZNode(path)
			if err != nil {
				return errors.Wrapf(err, "while creating %v parent", path)
			}
			continue
		}
		return nil
	}
}

// durableUpsertZNode reliably upserts a ZNode in presence of racing actors
// trying to delete ancestor directories of its path. It never fails with cause
// zk.ErrNoNode or zk.ErrNodeExists.
func (m *Model) durableUpsertZNode(path string, val []byte, flags int32) error {
	for {
		_, err := m.zkConn.Set(path, val, versionAny)
		if err != nil {
			if err != zk.ErrNoNode {
				return errors.Wrapf(err, "while setting %v", path)
			}
			err = m.durableCreateZNode(path, val, flags)
			if err != nil {
				if errors.Cause(err) == zk.ErrNodeExists {
					continue
				}
				return errors.WithStack(err)
			}
			return nil
		}
		return nil
	}
}

// durableCreateParentZNode reliably ensures that parent node of path exists in
// presence of racing actors trying to delete it. If some part of the path is
// missing it is recreated, so the function never fails with cause zk.ErrNoNode
// or zk.ErrNodeExists.
func (m *Model) durableCreateParentZNode(path string) error {
	slashIdx := strings.LastIndex(path, "/")
	if slashIdx < 0 {
		return errors.Errorf("Invalid path %v", path)
	}
	parentPath := path[:slashIdx]
	err := m.durableCreateZNode(parentPath, nil, 0)
	if err != nil && errors.Cause(err) != zk.ErrNodeExists {
		return errors.WithStack(err)
	}
	return nil
}

// recursiveDeleteZNode recursively deletes a subtree with root at path, but
// fails as soon as detects that racing actors create ZNodes inside the subtree,
// an error with cause zk.ErrNotEmpty is returned in this case.
func (m *Model) recursiveDeleteZNode(path string) error {
	// First delete all znode children recursively.
	children, stat, err := m.zkConn.Children(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil
		}
		return errors.Wrapf(err, "while getting %v's children", path)
	}
	for _, child := range children {
		childPath := path + "/" + child
		err = m.recursiveDeleteZNode(childPath)
		if err != nil && errors.Cause(err) != zk.ErrNoNode {
			return errors.WithStack(err)
		}
	}
	// When there are no more children delete the znode itself.
	err = m.zkConn.Delete(path, stat.Version)
	if err != nil && errors.Cause(err) != zk.ErrNoNode {
		return errors.Wrapf(err, "while deleting %v", path)
	}
	return nil
}

// watchZNodeChildren creates a watch on ZNode children. If the ZNode does not
// exist then it is durably created with empty value.
func (m *Model) watchZNodeChildren(path string) ([]string, <-chan zk.Event, error) {
	for {
		children, _, watchCh, err := m.zkConn.ChildrenW(path)
		if err != nil {
			if err != zk.ErrNoNode {
				return nil, nil, errors.Wrapf(err, "while watching %v's children", path)
			}
			// ZNode does not exist, so let's create it.
			err = m.durableUpsertZNode(path, nil, 0)
			if err != nil {
				return nil, nil, errors.WithStack(err)
			}
			continue
		}
		sort.Strings(children)
		return children, watchCh, nil
	}
}

// memberSpec structure is inherited from Java consumer, but we only use
// subscriptions and ignore the reset, besides only "static" pattern is
// supported, and it is not compatible with Kafka consumer group API.
type memberSpec struct {
	Subscription map[string]int `json:"subscription"`

	Pattern   string `json:"pattern"`
	Timestamp int64  `json:"timestamp"`
	Version   int    `json:"version"`
}

func newMemberSpec(topics []string) memberSpec {
	subscription := make(map[string]int)
	for _, topic := range topics {
		subscription[topic] = 1
	}
	return memberSpec{
		Subscription: subscription,

		Pattern:   "static",
		Timestamp: time.Now().Unix(),
		Version:   1,
	}
}

func (ms *memberSpec) topics() []string {
	topics := make([]string, 0, len(ms.Subscription))
	for topic := range ms.Subscription {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}
