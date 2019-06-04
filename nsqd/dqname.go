// +build !windows

package nsqd

func getBackendWriterName(topicName string, part int) string {
	return GetTopicFullName(topicName, part)
}

func getBackendReaderName(topicName string, part int, channelName string) string {
	// backend names, for uniqueness, automatically include the topic... <topic>:<channel>
	backendName := GetTopicFullName(topicName, part) + ":" + channelName
	return backendName
}

func getTopicBackendName(topicName string, partition int) string {
	return GetTopicFullName(topicName, partition)
}
