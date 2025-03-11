package cache

import (
	"errors"
	"github.com/saichler/reflect/go/reflect/properties"
	"github.com/saichler/reflect/go/reflect/updating"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
)

func CreateNotificationSet(t types.NotificationType, typeName, source string, changeCount int, sequence uint32) *types.NotificationSet {
	notificationSet := &types.NotificationSet{}
	notificationSet.TypeName = typeName
	notificationSet.Source = source
	notificationSet.Type = t
	notificationSet.NotificationList = make([]*types.Notification, changeCount)
	notificationSet.Sequence = sequence
	return notificationSet
}

func (this *Cache) createNotificationSet(t types.NotificationType, changeCount int) *types.NotificationSet {
	defer func() { this.sequence++ }()
	return CreateNotificationSet(t, this.typeName, this.source, changeCount, this.sequence)
}

func CreateAddNotification(any interface{}, typeName, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Add, typeName, source, changeCount, sequence)
	obj := object.NewEncode([]byte{}, 0)
	err := obj.Add(any)
	if err != nil {
		return nil, err
	}
	n := &types.Notification{}
	n.NewValue = obj.Data()
	notificationSet.NotificationList[0] = n
	return notificationSet, nil
}

func (this *Cache) createAddNotification(any interface{}) (*types.NotificationSet, error) {
	defer func() { this.sequence++ }()
	return CreateAddNotification(any, this.typeName, this.source, 1, this.sequence)
}

func CreateReplaceNotification(old, new interface{}, typeName, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Replace, typeName, source, 1, sequence)
	oldObj := object.NewEncode([]byte{}, 0)
	err := oldObj.Add(old)
	if err != nil {
		return nil, err
	}

	newObj := object.NewEncode([]byte{}, 0)
	err = newObj.Add(new)
	if err != nil {
		return nil, err
	}

	n := &types.Notification{}
	n.OldValue = oldObj.Data()
	n.NewValue = newObj.Data()
	notificationSet.NotificationList[0] = n
	return notificationSet, nil
}

func (this *Cache) createReplaceNotification(old, new interface{}) (*types.NotificationSet, error) {
	defer func() { this.sequence++ }()
	return CreateReplaceNotification(old, new, this.typeName, this.source, 1, this.sequence)
}

func CreateDeleteNotification(any interface{}, typeName, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Delete, typeName, source, 1, sequence)
	obj := object.NewEncode([]byte{}, 0)
	err := obj.Add(any)
	if err != nil {
		return nil, err
	}
	n := &types.Notification{}
	n.OldValue = obj.Data()
	notificationSet.NotificationList[0] = n
	return notificationSet, nil
}

func (this *Cache) createDeleteNotification(any interface{}) (*types.NotificationSet, error) {
	defer func() { this.sequence++ }()
	return CreateDeleteNotification(any, this.typeName, this.source, 1, this.sequence)
}

func CreateUpdateNotification(changes []*updating.Change, typeName, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Update, typeName, source, changeCount, sequence)
	for i, change := range changes {
		n := &types.Notification{}
		n.PropertyId = change.PropertyId()
		if change.OldValue() != nil {
			obj := object.NewEncode([]byte{}, 0)
			err := obj.Add(change.OldValue())
			if err != nil {
				return nil, err
			}
			n.OldValue = obj.Data()
		}
		if change.NewValue() != nil {
			obj := object.NewEncode([]byte{}, 0)
			err := obj.Add(change.NewValue())
			if err != nil {
				return nil, err
			}
			n.NewValue = obj.Data()
			notificationSet.NotificationList[i] = n
		}
	}
	return notificationSet, nil
}

func (this *Cache) createUpdateNotification(changes []*updating.Change) (*types.NotificationSet, error) {
	defer func() { this.sequence++ }()
	return CreateUpdateNotification(changes, this.typeName, this.source, len(changes), this.sequence)
}

func ItemOf(n *types.NotificationSet, i common.IIntrospector) (interface{}, error) {
	switch n.Type {
	case types.NotificationType_Replace:
		fallthrough
	case types.NotificationType_Add:
		obj := object.NewDecode(n.NotificationList[0].NewValue, 0, n.TypeName, i.Registry())
		v, e := obj.Get()
		return v, e
	case types.NotificationType_Delete:
		obj := object.NewDecode(n.NotificationList[0].OldValue, 0, n.TypeName, i.Registry())
		v, e := obj.Get()
		return v, e
	case types.NotificationType_Update:
		info, err := i.Registry().Info(n.TypeName)
		if err != nil {
			return nil, err
		}
		root, err := info.NewInstance()
		if err != nil {
			return nil, err
		}
		for _, notif := range n.NotificationList {
			p, e := properties.PropertyOf(notif.PropertyId, i)
			if e != nil {
				panic(e)
			}
			obj := object.NewDecode(notif.NewValue, 0, "", i.Registry())
			v, e := obj.Get()
			if e != nil {
				return nil, e
			}
			_, _, e = p.Set(root, v)
			if e != nil {
				return nil, e
			}
		}
		return root, nil
	}
	return nil, errors.New("Unknown notification type")
}
