# Raft - Implementation 

## Motivation

I am currently taking CS6650 at NEU (Scalable Distributed Systems). For the final project we are to choose a project of our own. I am not a very big fan of devs-op-ig, so I wanted to try out a more code-heavy, systems-level project instead of AWS-related deployments. 

This repository contains my personal implementation of the labs from MIT's 6.5840 (formerly 6.824) course on distributed systems. This was undertaken for fun and to deepen my understanding of the subject.

## Completed Work

As of now, Part 3 of the lab series is fully implemented.

All tests for these parts are passing (ran 20 times and all passed), but there might still be some missing edge cases.

## Future Work

I am considering extending this project to include:
- **Part 4:** Key/Value Service on top of Raft
- **Part 5:** Sharding

## Personal Takeaways

A lot of blood, sweat and tears over a week, but was very rewarding in terms of designing a relatively complex system (who would have guessed 800 lines of code can be so difficult?). It's also been a while since I have completely hand written a whole repo from scratch without any LLM use, so it was fun. Still, I don't think I have a super solid grasp on why Raft is able to handle all the failures and edge cases (I did develop more of a solid intuition). All in all, a great coding exercise.