package cache

import (
	"errors"
	"github.com/saichler/reflect/go/reflect/property"
	"github.com/saichler/reflect/go/reflect/updater"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/shared/go/share/interfaces"
	"github.com/saichler/shared/go/types"
)

func (this *Cache) createNotificationSet(t types.NotificationType, l int) *types.NotificationSet {
	notificationSet := &types.NotificationSet{}
	notificationSet.TypeName = this.typeName
	notificationSet.Source = this.source
	notificationSet.Type = t
	notificationSet.NotificationList = make([]*types.Notification, l)
	this.mtx.Lock()
	notificationSet.Sequence = this.sequence
	this.sequence++
	this.mtx.Unlock()
	return notificationSet
}

func (this *Cache) createAddNotification(any interface{}) (*types.NotificationSet, error) {
	notificationSet := this.createNotificationSet(types.NotificationType_Add, 1)
	obj := object.New([]byte{}, 0, "", this.introspector.Registry())
	err := obj.Add(any)
	if err != nil {
		return nil, err
	}
	n := &types.Notification{}
	n.NewValue = obj.Data()
	notificationSet.NotificationList[0] = n
	return notificationSet, nil
}

func (this *Cache) createReplaceNotification(any interface{}) (*types.NotificationSet, error) {
	notificationSet := this.createNotificationSet(types.NotificationType_Replace, 1)
	obj := object.New([]byte{}, 0, "", this.introspector.Registry())
	err := obj.Add(any)
	if err != nil {
		return nil, err
	}
	n := &types.Notification{}
	n.NewValue = obj.Data()
	notificationSet.NotificationList[0] = n
	return notificationSet, nil
}

func (this *Cache) createDeleteNotification(any interface{}) (*types.NotificationSet, error) {
	notificationSet := this.createNotificationSet(types.NotificationType_Delete, 1)
	obj := object.New([]byte{}, 0, "", this.introspector.Registry())
	err := obj.Add(any)
	if err != nil {
		return nil, err
	}
	n := &types.Notification{}
	n.OldValue = obj.Data()
	notificationSet.NotificationList[0] = n
	return notificationSet, nil
}

func (this *Cache) createUpdateNotification(changes []*updater.Change) (*types.NotificationSet, error) {
	notificationSet := this.createNotificationSet(types.NotificationType_Update, len(changes))
	for i, change := range changes {
		n := &types.Notification{}
		n.PropertyId = change.PropertyId()
		obj := object.New([]byte{}, 0, "", this.introspector.Registry())
		err := obj.Add(change.OldValue())
		if err != nil {
			return nil, err
		}
		n.OldValue = obj.Data()

		obj = object.New([]byte{}, 0, "", this.introspector.Registry())
		err = obj.Add(change.NewValue())
		if err != nil {
			return nil, err
		}
		n.NewValue = obj.Data()
		notificationSet.NotificationList[i] = n
	}
	return notificationSet, nil
}

func ItemOf(n *types.NotificationSet, i interfaces.IIntrospector) (interface{}, error) {
	switch n.Type {
	case types.NotificationType_Replace:
		fallthrough
	case types.NotificationType_Add:
		obj := object.New(n.NotificationList[0].NewValue, 0, "", i.Registry())
		v, e := obj.Get()
		return v, e
	case types.NotificationType_Delete:
		obj := object.New(n.NotificationList[0].OldValue, 0, "", i.Registry())
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
			p, e := property.PropertyOf(notif.PropertyId, i)
			if e != nil {
				panic(e)
			}
			obj := object.New(notif.NewValue, 0, "", i.Registry())
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
