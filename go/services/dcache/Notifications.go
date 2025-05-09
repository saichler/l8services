package dcache

import (
	"errors"
	"github.com/saichler/l8srlz/go/serialize/object"
	"github.com/saichler/l8types/go/ifs"
	"github.com/saichler/l8types/go/types"
	"github.com/saichler/reflect/go/reflect/properties"
	"github.com/saichler/reflect/go/reflect/updating"
)

func CreateNotificationSet(t types.NotificationType, serviceName, key string, serviceArea uint16, modelType, source string,
	changeCount int, sequence uint32) *types.NotificationSet {
	notificationSet := &types.NotificationSet{}
	notificationSet.ServiceName = serviceName
	notificationSet.ServiceArea = int32(serviceArea)
	notificationSet.ModelType = modelType
	notificationSet.Source = source
	notificationSet.Type = t
	notificationSet.NotificationList = make([]*types.Notification, changeCount)
	notificationSet.Sequence = sequence
	notificationSet.ModelKey = key
	return notificationSet
}

func (this *DCache) createNotificationSet(t types.NotificationType, key string, changeCount int) *types.NotificationSet {
	defer func() { this.sequence++ }()
	return CreateNotificationSet(t, this.serviceName, key, this.serviceArea, this.modelType, this.source, changeCount, this.sequence)
}

func CreateAddNotification(any interface{}, serviceName, key string, serviceArea uint16, modelType, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Add, serviceName, key, serviceArea, modelType, source, changeCount, sequence)
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

func CreateSyncNotification(any interface{}, serviceName, key string, serviceArea uint16, modelType, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Sync, serviceName, key, serviceArea, modelType, source, changeCount, sequence)
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

func (this *DCache) createAddNotification(any interface{}, key string) (*types.NotificationSet, error) {
	defer func() { this.sequence++ }()
	return CreateAddNotification(any, this.serviceName, key, this.serviceArea, this.modelType, this.source, 1, this.sequence)
}

func (this *DCache) createSyncNotification(any interface{}, key string) (*types.NotificationSet, error) {
	defer func() { this.sequence++ }()
	return CreateSyncNotification(any, this.serviceName, key, this.serviceArea, this.modelType, this.source, 1, this.sequence)
}

func CreateReplaceNotification(old, new interface{}, serviceName, key string, serviceArea uint16, modelType, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Replace, serviceName, key, serviceArea, modelType, source, 1, sequence)
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

func (this *DCache) createReplaceNotification(old, new interface{}, key string) (*types.NotificationSet, error) {
	defer func() { this.sequence++ }()
	return CreateReplaceNotification(old, new, this.serviceName, key, this.serviceArea, this.modelType, this.source, 1, this.sequence)
}

func CreateDeleteNotification(any interface{}, serviceName, key string, serviceArea uint16, modelType, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Delete, serviceName, key, serviceArea, modelType, source, 1, sequence)
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

func (this *DCache) createDeleteNotification(any interface{}, key string) (*types.NotificationSet, error) {
	defer func() { this.sequence++ }()
	return CreateDeleteNotification(any, this.serviceName, key, this.serviceArea, this.modelType, this.source, 1, this.sequence)
}

func CreateUpdateNotification(changes []*updating.Change, serviceName, key string, serviceArea uint16, modelType, source string, changeCount int, sequence uint32) (*types.NotificationSet, error) {
	notificationSet := CreateNotificationSet(types.NotificationType_Update, serviceName, key, serviceArea, modelType, source, changeCount, sequence)
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
		}
		notificationSet.NotificationList[i] = n
	}
	return notificationSet, nil
}

func (this *DCache) createUpdateNotification(changes []*updating.Change, key string) (*types.NotificationSet, error) {
	defer func() { this.sequence++ }()
	return CreateUpdateNotification(changes, this.serviceName, key, this.serviceArea, this.modelType, this.source, len(changes), this.sequence)
}

func ItemOf(n *types.NotificationSet, i ifs.IIntrospector) (interface{}, error) {
	switch n.Type {
	case types.NotificationType_Replace:
		fallthrough
	case types.NotificationType_Sync:
		fallthrough
	case types.NotificationType_Add:
		obj := object.NewDecode(n.NotificationList[0].NewValue, 0, i.Registry())
		v, e := obj.Get()
		return v, e
	case types.NotificationType_Delete:
		obj := object.NewDecode(n.NotificationList[0].OldValue, 0, i.Registry())
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
			var value interface{}
			if notif.NewValue != nil {
				obj := object.NewDecode(notif.NewValue, 0, i.Registry())
				v, e1 := obj.Get()
				if e1 != nil {
					return nil, e1
				}
				value = v
			}
			_, _, e = p.Set(root, value)
			if e != nil {
				return nil, e
			}
		}
		return root, nil
	}
	return nil, errors.New("Unknown notification type")
}
