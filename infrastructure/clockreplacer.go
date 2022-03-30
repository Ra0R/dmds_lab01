package infrastructure

type FrameID int

// ClockReplacer the data needed for the clock replacer algorithm
type ClockReplacer struct {
	cList     *circularList // circular list of frames. Value = true
	clockHand **node        // node in the circular list we are currently at
}

// ChooseVictim removes the victim frame, i.e. frame corresponding to the next node with value false
// If value is true, set it to false to allow for a "second chance" – so algorithm goes  at most around the whole list once
func (clockReplacer *ClockReplacer) ChooseVictim() *FrameID {
	if clockReplacer.cList.size == 0 {
		return nil
	}

	var victimFrameID *FrameID
	currentNode := *clockReplacer.clockHand
	for {
		if currentNode.value.(bool) {
			currentNode.value = false
			clockReplacer.clockHand = &currentNode.next
		} else {
			frameID := currentNode.key.(FrameID)
			victimFrameID = &frameID

			clockReplacer.clockHand = &currentNode.next

			clockReplacer.cList.remove(currentNode.key)
			return victimFrameID
		}
	}
}

// Unpin unpins a frame, indicating that it can now be victimized
func (clockReplacer *ClockReplacer) Unpin(id FrameID) {
	if !clockReplacer.cList.hasKey(id) {
		clockReplacer.cList.insert(id, true)
		if clockReplacer.cList.size == 1 {
			clockReplacer.clockHand = &clockReplacer.cList.head
		}
	}
}

// Pin pins a frame, indicating that it should not be victimized until it is unpinned
func (clockReplacer *ClockReplacer) Pin(id FrameID) {
	node := clockReplacer.cList.find(id)
	if node == nil {
		return
	}

	if (*clockReplacer.clockHand) == node {
		clockReplacer.clockHand = &(*clockReplacer.clockHand).next
	}
	clockReplacer.cList.remove(id)

}

// Size returns the size of the clock
func (clockReplacer *ClockReplacer) Size() int {
	return clockReplacer.cList.size
}

// NewClockReplacer instantiates a new clock replacer
func NewClockReplacer(poolSize int) *ClockReplacer {
	cList := newCircularList(poolSize)
	return &ClockReplacer{cList, &cList.head}
}
