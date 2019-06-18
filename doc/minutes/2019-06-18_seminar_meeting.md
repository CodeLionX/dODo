# Seminar Meeting 2019-06-18

## Recap last tasks

| Status | Who?  | Until when?   | What? |
| :----: | :---: | :------------ | :---- |
| :heavy_check_mark: | Sebi | 18.06. | Find out why we only use far below 100% of CPU power |
| :x: | Jul | 18.06. | Think about generating all candidates (fix the pruning errors of OCDDISCOVER) |
| :heavy_check_mark: | Jul | 18.06. | Implement work stealing protocol with [Akka PubSub](https://doc.akka.io/docs/akka/current/distributed-pub-sub.html) for broadcasting |
| :x: |  | somewhen | Check, why we only need 40seconds for the flight_1k dataset, instead of over 5h -- maybe it's because of our `null`-handling |
| :x: |  |  | Create protocol document for work stealing protocol |
| :x: |  |  | Create protocol document for state synchronization protocol |
| :x: |  |  | Protocol document: how do we handle joining nodes or overdue messages during the recovery phase? |

## Midterm Presentation Feedback

- for the final presentation: change speaker roles (start / end)
- Slides, Q&A and Presentation all perfect!

## System Coordinator

- we do not need the `SystemCoordinator` actor anymore
- downing and time measurement responsibility is taken over by master actor
- actual evaluation time measurements will be taken with external tools

## Downing Protocol

- Each master actor has to decide if the system finished all work.
- Start downing protocol when
  - no worker processes any items (empty pending candidate queue)
  - empty local work queue
  - work stealing reveals that other nodes have no work as well
- Downing protocol (voting):
  - tbd
- If cluster decides on algorithm finished: each master stops all actors in his own actor system (so reaper can terminate the system)

## Next Tasks

| Who?  | Until when?   | What? |
| :---: | :------------ | :---- |
| Jul | 25.06. | Think about generating all candidates (fix the pruning errors of OCDDISCOVER) |
| Jul | 25.06. | Test work stealing protocol and make sure we do not ask multiple times for work |
| Sebi | 25.06. | Use data holder side channel to request data if no filepath was specified (using CLI) --> also update readme |
| Sebi | 25.06. | Specify Downing Protocol |
|  | somewhen | Check, why we only need 40seconds for the flight_1k dataset, instead of over 5h -- maybe it's because of our `null`-handling |
|  |  | Create protocol document for work stealing protocol |
|  |  | Create protocol document for state synchronization protocol |
|  |  | Protocol document: how do we handle joining nodes or overdue messages during the recovery phase? |
