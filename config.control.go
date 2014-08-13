package gocassos

func (c *ObjectStorage) Wait() {
	if c != nil {
		c.in_progress.Wait()
	}
}
