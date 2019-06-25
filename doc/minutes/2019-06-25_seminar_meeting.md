# Seminar Meeting 2019-06-25

## Recap last tasks

| Status | Who?  | Until when?   | What? |
| :----: | :---: | :------------ | :---- |
|  | Jul | 25.06. | Think about generating all candidates (fix the pruning errors of OCDDISCOVER) |
| :heavy_check_mark: | Jul | 25.06. | Test work stealing protocol and make sure we do not ask multiple times for work |
| :heavy_check_mark: | Sebi | 25.06. | Use data holder side channel to request data if no filepath was specified (using CLI) --> also update readme |
| :heavy_check_mark: | Sebi, Jul | 24.06. | Specify Downing Protocol |
| :heavy_check_mark: | Sebi | 24.06. | Share reduced columns with joining nodes |
| :x: |  | somewhen | Check, why we only need 40seconds for the flight_1k dataset, instead of over 5h -- maybe it's because of our `null`-handling |
| :x: |  |  | Create protocol document for work stealing protocol |
| :x: |  |  | Create protocol document for state synchronization protocol |
| :x: |  |  | Protocol document: how do we handle joining nodes or overdue messages during the recovery phase? |

## Final Presentation - Do we need performance metrics

- testing on the local system should be included (one node setup)
- seeing that we utilize the node
- it doesn't matter if we are not quite finished yet (full coverage)
- so tests across multiple system must not be included
- live demo is desired

## Remarks for final paper

- at the end (future work or so) talk about all the points
  - where we were not able to implement them
  - that raise interesting questions
  - where we found interesting problems

## How to ensure work stealing works

- Watch actor ref of thief in sender
- Send work over
- If ACK received de-watch thief
- If `Terminated` received, recover work to own queue
- Use `akka.actor.remote.artery.transport = tcp` as transport configuration

## Implementation of State-Sync Protocol

- don't store the state of the other nodes in the master actor, but use a separate actor on each nodes that handles the state replication / synchronization (one on each node)
- let's call it `StateReplicator`
- handles request of master to replicate state
- reacts on other nodes failures and sends recovered work to local master
- if a new node joins it communicates with cluster listener to figure out if it is a neighbor and re-syncs the states
- protocol specification is described in [meeting notes from 23.05.2019](./2019-05-23_meeting.md)

## Next Tasks

| Who?  | Until when?   | What? |
| :---: | :------------ | :---- |
| Sebi | 02.07. | Fix issue "Handle delayed/lost `AckWorkReceived`" (#51) as described in issue |
| Jul | 02.07. | Implement downing protocol (#50) |
| Sebi | 02.07. | Change sidechannel handling to use `SourceRef`s instead of explicitly binding to a TCP socket. [Akka StreamRefs](https://doc.akka.io/docs/akka/current/stream/stream-refs.html#source-refs-offering-streaming-data-to-a-remote-system) |
| Jul | 02.07. | Think about generating all candidates (fix the pruning errors of OCDDISCOVER) |
|  | after presentation | Implement master state synchronization |
|  | somewhen | Check, why we only need 40seconds for the flight_1k dataset, instead of over 5h -- maybe it's because of our `null`-handling |
|  |  | Create protocol document for work stealing protocol |
|  |  | Create protocol document for state synchronization protocol |
|  |  | Protocol document: how do we handle joining nodes or overdue messages during the recovery phase? |
