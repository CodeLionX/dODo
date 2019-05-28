# Seminar Meeting 2019-05-28

## Recap last tasks

| Status | Who?  | Until when?   | What? |
| :----: | :---: | :------------ | :---- |
| :heavy_check_mark: | Sebi | 28.05. | Do not throw exception on EOF if file is empty before inference number is reached |
| :heavy_check_mark: | Sebi | 28.05. | Use column names (parsed from CSV or synthetic) for output |
| :heavy_check_mark: | Sebi | 28.05. | test all available datasets to be compatible to our algorithm and fix issues |
| :heavy_check_mark: | Jul  | 28.05. | write E2E (acceptance) test for our test data set |

## Input from Thorsten

- we are invited to the Kafka Seminar presentations
  - intermediate presentation: Wednesday, 12.06. 11:00 Uhr
  - final presentation:, Wednesday 17.07. 11:00 Uhr
- Prof. Naumann presented in the Web Table Seminar, what he thinks, that is important for presentations: [Slides](https://hpi.de/fileadmin/user_upload/fachgebiete/naumann/lehre/SS2019/PWT/Literature_and_Presentation.pdf)
- slides from Thorsten (29.05) about research work could also be interesting for us (_slides link tbd_)

## Feedback to our approach

- what happens, if three neighbouring nodes fail at the same time?
  - sanity check running in all nodes (checking the above condition)? --> restart the whole system
  - we do not consider this failure type --> manual restart needed
- we could improve the algorithm's state handling by just storing a marker for "candidate doesn't hold" and generate the following candidates on-demand later on
  - could improve memory usage
  - leads to less data being stored in the replicas and needed to be send across the network (less synchronization data)
- Option for `null`-handling is OK (if we want to optimize the algorithm, we should consider this; this is not the seminar goal)

## Intermediate Presentation

- important: sound story, comprehensible
- consider the short time: 15min
- place: F2.10
- Thorsten suggest speaking englisch
- slides
  - always in english
  - HPI template
  - **Do not forget our names!**
  - few text
  - schematic drawings and diagrams
- introduction:
  - topic, what is our goal?
  - always assume, that somebody has no idea of it:
    what is Akka, actor programming, ...?
    explain all technical terms shortly (what is the core idea?)
- do not end on "Thank you"-slide: summary slide, which initiates a discussion
- clarify requirements / setting at the beginning: How important is fault-tolerance / reliability, ...?
- focus on one main contribution and take the other topics as a given;
  a complete explanation of the system is only possible in the final report
- balanced speaking times for both team members (disregarding, which topic is important in our opinion)

## Next Tasks

| Who?  | Until when?   | What? |
| :---: | :------------ | :---- |
| Jul | 01.06. | Batch work and results for workers inside the actor systems to reduce message load on master actor |
| Sebi | 01.06. | Side-channel for data set exchange between actor systems (using Netty) |
