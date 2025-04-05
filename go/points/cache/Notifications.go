package cache

import (
	"errors"
	"github.com/saichler/reflect/go/reflect/properties"
	"github.com/saichler/reflect/go/reflect/updating"
	"github.com/saichler/serializer/go/serialize/object"
	"github.com/saichler/types/go/common"
	"github.com/saichler/types/go/types"
)

func CreateNotificationSet(t types.NotificationType, serviceName string, serviceArea int32, modelType, source string,
	changeCount int, sequence uint32) *types.NotificationSet {
	notificationSet := &types.NotificationSet{}
	notificationSet.ServiceName = serviceName
	notificationSet.ServiceArea = serviceArea
	notificationSet.ModelType = modelType
	notificationSet.Source = source
	notificationSet.Type = t
	notificationSet.NotificationList = make([]*types.Notification, changeCount)
	notificationSet.Sequence = sequence
	return notificationSet
}

func (this *Cache) createNotificationSet(t types.NotificationType, changeCount int) *types.NotificationSet {
	defer func() { this.sequence++ }()
	return CreateNotificationSet(t, this.serviceName, this.serviceArea, this.modelType, this.source, changeCount, this.sequence)
}

func CreateAddNotification(any interface{}, serviceName string, serviceArea int32, modelType, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Add, serviceName, serviceArea, modelType, source, changeCount, sequence)
	obj := object.NewEncode()
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
	return CreateAddNotification(any, this.serviceName, this.serviceArea, this.modelType, this.source, 1, this.sequence)
}

func CreateReplaceNotification(old, new interface{}, serviceName string, serviceArea int32, modelType, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Replace, serviceName, serviceArea, modelType, source, 1, sequence)
	oldObj := object.NewEncode()
	err := oldObj.Add(old)
	if err != nil {
		return nil, err
	}

	newObj := object.NewEncode()
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
	return CreateReplaceNotification(old, new, this.serviceName, this.serviceArea, this.modelType, this.source, 1, this.sequence)
}

func CreateDeleteNotification(any interface{}, serviceName string, serviceArea int32, modelType, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Delete, serviceName, serviceArea, modelType, source, 1, sequence)
	obj := object.NewEncode()
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
	return CreateDeleteNotification(any, this.serviceName, this.serviceArea, this.modelType, this.source, 1, this.sequence)
}

func CreateUpdateNotification(changes []*updating.Change, serviceName string, serviceArea int32, modelType, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Update, serviceName, serviceArea, modelType, source, changeCount, sequence)
	for i, change := range changes {
		n := &types.Notification{}
		n.PropertyId = change.PropertyId()
		if change.OldValue() != nil {
			obj := object.NewEncode()
			err := obj.Add(change.OldValue())
			if err != nil {
				return nil, err
			}
			n.OldValue = obj.Data()
		}
		if change.NewValue() != nil {
			obj := object.NewEncode()
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
	return CreateUpdateNotification(changes, this.serviceName, this.serviceArea, this.modelType, this.source, len(changes), this.sequence)
}

func ItemOf(n *types.NotificationSet, i common.IIntrospector) (interface{}, error) {
	location := 0
	switch n.Type {
	case types.NotificationType_Replace:
		fallthrough
	case types.NotificationType_Add:
		obj := object.NewDecode(&n.NotificationList[0].NewValue, &location, i.Registry())
		v, e := obj.Get()
		return v, e
	case types.NotificationType_Delete:
		obj := object.NewDecode(&n.NotificationList[0].OldValue, &location, i.Registry())
		v, e := obj.Get()
		return v, e
	case types.NotificationType_Update:
		info, err := i.Registry().Info(n.ModelType)
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
			obj := object.NewDecode(&notif.NewValue, &location, i.Registry())
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
