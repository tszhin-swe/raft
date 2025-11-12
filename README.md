# MIT 6.5840 Labs - Implementation

This repository contains my personal implementation of the labs from MIT's 6.5840 (formerly 6.824) course on distributed systems. This was undertaken for fun and to deepen my understanding of the subject.

## Completed Work

As of now, Part 3 of the lab series is fully implemented. This includes:
- Lab 3A: Key/value service without snapshots
- Lab 3B: Key/value service with snapshots

All tests for these parts are passing (ran 20 times and all passed), but there might still be some missing edge cases.

## Future Work

I am considering extending this project to include:
- **Part 4:** Sharded Key/Value Service
- **Part 5:** Persistence

## Personal Takeaways

A lot of blood, sweat and tears over a week, but was very rewarding in terms of designing a relatively complex system (who would have guessed 800 lines of code can be so difficult?). Still, I don't think I have a super solid grasp on why Raft is able to handle all the failures and edge cases (more of a solid intuition).  