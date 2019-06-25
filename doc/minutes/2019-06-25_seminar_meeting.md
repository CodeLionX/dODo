# Seminar Meeting 2019-06-25

## Recap last tasks

| Status | Who?  | Until when?   | What? |
| :----: | :---: | :------------ | :---- |
|  | Jul | 25.06. | Think about generating all candidates (fix the pruning errors of OCDDISCOVER) |
|  | Jul | 25.06. | Test work stealing protocol and make sure we do not ask multiple times for work |
|  | Sebi | 25.06. | Use data holder side channel to request data if no filepath was specified (using CLI) --> also update readme |
|  | Sebi | 25.06. | Specify Downing Protocol |
|  |  | somewhen | Check, why we only need 40seconds for the flight_1k dataset, instead of over 5h -- maybe it's because of our `null`-handling |
|  |  |  | Create protocol document for work stealing protocol |
|  |  |  | Create protocol document for state synchronization protocol |
|  |  |  | Protocol document: how do we handle joining nodes or overdue messages during the recovery phase? |

## Final Presentation - Do we need performance metrics

- testing on the local system should be included
- seeing that we utilize the node
- it doesn't matter if we are not quite finished yet
- so tests across multiple system must not be included

## Remarks for final paper

- add the end (future work or so) talk about all the points
  - where we were not able to implement them
  - that raise interesting questions
  - where we found interesting problems

## How to ensure work stealing worked

- Watch actor ref of thief in sender
- Send work over
- If ACK received de-watch thief
- If `Terminated` received, recover work to own queue
- Use `akka.actor.remote.artery.transport = tcp` as transport configuration

## Implementation of State-Sync Protocol

## Next Tasks

| Who?  | Until when?   | What? |
| :---: | :------------ | :---- |
||||