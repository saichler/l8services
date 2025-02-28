package transaction

import "github.com/saichler/shared/go/types"

type Transaction struct {
	id  string
	set *types.NotificationSet
}
