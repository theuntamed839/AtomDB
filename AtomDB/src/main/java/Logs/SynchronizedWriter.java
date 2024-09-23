package Logs;

public sealed interface SynchronizedWriter extends Writer permits SynchronizedFileChannelWriter {
}
