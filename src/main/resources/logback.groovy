import ch.qos.logback.classic.filter.ThresholdFilter

statusListener(OnConsoleStatusListener)

appender("FILE", FileAppender) {
  file = "teetime.log"
  append = false
  filter(ThresholdFilter) {
	level = INFO
  }
  encoder(PatternLayoutEncoder) {
    pattern = "%msg%n"
  }
}

appender("CONSOLE", ConsoleAppender) {
  encoder(PatternLayoutEncoder) {
    pattern = "%d{HH:mm:ss.SSS} %level %logger - %msg%n"
  }
}

root WARN, ["CONSOLE"]
