import os
import shutil
from datetime import datetime, timedelta

import statistics
from parse import parse

ogLogsDir = "logs/"
sanitisedLogsDir = "sLogs/"
ogLogFiles = ["1.log", "2.log", "3.log", "4.log", "5.log"]
timelineFile = "timeline.log"

Stats = {}


def statsOfThreads():
    if "statsOfThreadResults" in Stats.keys():
        print("(Pre-calculated)")
        print("Average lifecycle of threads:", Stats["statsOfThreadResults"][0])
        print("Standard deviation of threads:", Stats["statsOfThreadResults"][1])
        return

    linePattern = "{ogFile}:::{pId}:::{threadId}:::{startTime}:::{endTime}"
    readFile = open(sanitisedLogsDir + timelineFile, "r")
    lifetimeOfThreads = []
    while True:
        line = readFile.readline()
        if not line:
            break

        parsed = parse(linePattern, line)
        if not parsed:
            continue
        threadStartTime = datetime.strptime(parsed["startTime"].split(",")[0], '%Y-%m-%d %H:%M:%S')
        threadEndTime = datetime.strptime(parsed["endTime"].split(",")[0], '%Y-%m-%d %H:%M:%S')
        lifetimeOfThreads.append((threadEndTime - threadStartTime).total_seconds())
    Stats["statsOfThreadResults"] = (statistics.mean(lifetimeOfThreads), statistics.stdev(lifetimeOfThreads))
    print("Average lifecycle of threads:", Stats["statsOfThreadResults"][0])
    print("Standard deviation of threads:", Stats["statsOfThreadResults"][1])


def highestConcurrentThreads():

    if "highestConcurrentTime" in Stats.keys():
        print("(Pre-calculated)")
        print("Highest concurrent threads at time", Stats["highestConcurrentTime"])
        return
    print("# Program restarted after sanitisation of logs. Hence recalculating. It can also be stored in file.")
    linePattern = "{ogFile}:::{pId}:::{threadId}:::{startTime}:::{endTime}"
    threadCount = {}
    readFile = open(sanitisedLogsDir + timelineFile, "r")
    while True:
        line = readFile.readline()
        if not line:
            break

        parsed = parse(linePattern, line)
        if not parsed:
            continue
        threadStartTime = datetime.strptime(parsed["startTime"].split(",")[0], '%Y-%m-%d %H:%M:%S')
        threadEndTime = datetime.strptime(parsed["endTime"].split(",")[0], '%Y-%m-%d %H:%M:%S')
        while threadEndTime > threadStartTime:
            if str(threadStartTime) in threadCount.keys():
                threadCount[str(threadStartTime)] += 1
            else:
                threadCount[str(threadStartTime)] = 1
            threadStartTime += timedelta(seconds=1)
    secondWithMaxThreads = max(threadCount, key=threadCount.get)
    print("Highest concurrent threads at time", secondWithMaxThreads, threadCount[secondWithMaxThreads])


# Active threads in the given time range
def threadsInTimeRange(givenStartTime, givenEndTime):
    linePattern = "{ogFile}:::{pId}:::{threadId}:::{startTime}:::{endTime}"
    startTime = datetime.strptime(givenStartTime, '%Y-%m-%d %H:%M:%S')
    endTime = datetime.strptime(givenEndTime, '%Y-%m-%d %H:%M:%S')
    threadIds = []
    pIds = []
    ogFiles = []
    relaventLogs = []

    readFile = open(sanitisedLogsDir + timelineFile, "r")
    while True:
        line = readFile.readline()
        if not line:
            break

        parsed = parse(linePattern, line)
        if not parsed:
            continue
        threadStartTime = datetime.strptime(parsed["startTime"].split(",")[0], '%Y-%m-%d %H:%M:%S')
        threadEndTime = datetime.strptime(parsed["endTime"].split(",")[0], '%Y-%m-%d %H:%M:%S')
        if (threadStartTime < startTime < threadEndTime) or (
                startTime < threadStartTime < endTime):
            threadIds.append(parsed["threadId"])
            pIds.append(parsed["pId"])
            ogFiles.append(parsed["ogFile"])
            relaventLogs.append(line[:-1])

    readFile.close()
    print("start time", startTime, startTime.time())
    print("end time", endTime, endTime.time())
    print("active threads>>", len(set(threadIds)))
    print("thread Id>>", set(threadIds))
    print("process Ids>>", set(pIds))
    print("ogFiles>>", set(ogFiles))
    print("relaventLogs", len(relaventLogs), relaventLogs)
    print()


def writeInFile(f, content, scope):
    writeFile = open(f, scope)
    writeFile.write(content)
    writeFile.close()


def createTimeline():
    writeFile = open(sanitisedLogsDir + timelineFile, "a+")
    dirs = [x[0] for x in os.walk(sanitisedLogsDir)]
    startPattern = "{ogFile}:::{pId}:::{threadId}:::{threadName}:::{timestamp}:::**START**"
    endPattern = "{ogFile}:::{pId}:::{threadId}:::{threadName}:::{timestamp}:::**END**"

    threadCount = {}
    lifetimeOfThreads = []

    for pDir in dirs:
        if pDir == sanitisedLogsDir:
            continue
        readFiles = os.listdir(pDir)
        for readFile in readFiles:
            f = open(pDir + "/" + readFile, "r")
            startTime = None
            endTime = None
            threadId = None
            ogFile = None
            while True:
                line = f.readline()
                if not line:
                    break
                startParsed = parse(startPattern, line)
                endParsed = parse(endPattern, line)

                if startParsed:
                    startTime = startParsed["timestamp"]
                    threadId = startParsed["threadId"]
                    ogFile = startParsed["ogFile"]
                    pId = startParsed["pId"]
                if endParsed and threadId == endParsed["threadId"]:
                    endTime = endParsed["timestamp"]

                if startTime and endTime:
                    writeFile.write("{}:::{}:::{}:::{}:::{}\n".format(ogFile, pId, threadId, startTime, endTime))

                    # Find max time
                    threadStartTime = datetime.strptime(startTime.split(",")[0], '%Y-%m-%d %H:%M:%S')
                    threadEndTime = datetime.strptime(endTime.split(",")[0], '%Y-%m-%d %H:%M:%S')
                    lifetimeOfThreads.append((threadEndTime - threadStartTime).total_seconds())

                    while threadEndTime > threadStartTime:
                        if str(threadStartTime) in threadCount.keys():
                            threadCount[str(threadStartTime)] += 1
                        else:
                            threadCount[str(threadStartTime)] = 1
                        threadStartTime += timedelta(seconds=1)

                    startTime = None
                    endTime = None
                    threadId = None
                    ogFile = None
    writeFile.close()

    secondWithMaxThreads = max(threadCount, key=threadCount.get)
    Stats["highestConcurrentTime"] = (secondWithMaxThreads, threadCount[secondWithMaxThreads])
    print("highestConcurrentTime of threads", Stats["highestConcurrentTime"])
    Stats["statsOfThreadResults"] = (statistics.mean(lifetimeOfThreads), statistics.stdev(lifetimeOfThreads))

    print("statsOfThreadResults, mean", Stats["statsOfThreadResults"][0])
    print("statsOfThreadResults, stddev", Stats["statsOfThreadResults"][1])


def sanitiseLogs():
    pattern = "{pId}:{threadId}::{threadName} {date} {timestamp} - {log}"
    for logFile in ogLogFiles:
        readFile = open(ogLogsDir + logFile, "r")
        workingFile = ""
        while True:
            line = readFile.readline()
            if not line:
                break
            parsed = parse(pattern, line)
            if not parsed:
                writeInFile(workingFile, line.replace("\n", " "), "a")
                continue
            if not os.path.exists(sanitisedLogsDir + parsed["pId"]):
                os.makedirs(sanitisedLogsDir + parsed["pId"])
            workingFile = sanitisedLogsDir + parsed["pId"] + "/" + parsed["threadId"] + ".log"
            writeInFile(workingFile,
                        "\n{}:::{}:::{}:::{}:::{} {}:::{}".format(
                            logFile,
                            parsed["pId"],
                            parsed["threadId"],
                            parsed["threadName"],
                            parsed["date"], parsed["timestamp"],
                            parsed["log"]),
                        "a+")
        readFile.close()


def init():
    if not os.path.exists(sanitisedLogsDir):
        os.makedirs(sanitisedLogsDir)
        print("Sanitising Logs, please wait...")
        sanitiseLogs()
        print("Creating timeline, please wait...")
        createTimeline()
        print("timeline prepared!")


if __name__ == "__main__":
    init()
    while True:
        print("You can do following operations:\n"
              "1) Threads in time range?\n"
              "2) Highest count of concurrent threads alive in a second?\n"
              "3) Avg and stdev of the threads lifetime?\n"
              "4) Suggestions to improve the logging system?\n"
              "5) Remake sanitised files and timeline\n"
              "6) Exit")
        query = input("Enter from 1-6?")
        print(query)
        if query == 1:
            t1 = raw_input("Start Time")
            t2 = raw_input("End Time")
            threadsInTimeRange(t1, t2)
        elif query == 2:
            highestConcurrentThreads()
        elif query == 3:
            statsOfThreads()
        elif query == 4:
            print("Logging system can be improved in multiple ways:\n"
                  "1) Flush logs only when thread ends. It will ensure all logs of a thread are together in the first place\n"
                  "2) Add another log for lifecycle of thread in an entirely new file\n"
                  "3) Use ELK stack. Logstash will push the real time logs in Elastic Search. Elastic search will then index the logs as per the given scheme. Those logs can then be visualised on Kibana in almost real time.\n"
                  "4) Always output a json of logs\n"
                  "5) Keep message enclosed with quotes\n"
                  "6) It's quite an open ended problem. Can have a lot of solutions.\n")
        elif query == 5:
            try:
                shutil.rmtree(sanitisedLogsDir)
            except OSError as e:
                print("Error: {} : {}".format(sanitisedLogsDir, e.strerror))
            init()

        elif query == 6:
            break
        else:
            print("invalid input")
