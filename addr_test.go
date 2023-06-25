package kafka

import "testing"

func TestAddr(t *testing.T) {
	t.Run("when zero value", func(t *testing.T) {
		got := addr("")
		if got != nil {
			t.Errorf("\nwanted nil\ngot    %T", got)
		}
	})

	t.Run("when non-zero", func(t *testing.T) {
		got := addr("id")
		wanted := "id"
		if wanted != *got {
			t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
		}
	})
}
