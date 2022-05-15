# Frequently asked questions

## What's the current status of PranaDB?

PranaDB is currently a work in progress and a technical preview. It's already pretty solid, but we are not feature
complete and we haven't done much performance optimisation yet. To date, we have mainly been working on getting the
fundamentals of the architecture right.

## How does PranaDB compare with other products on the market

Unlike other systems which have some similarities, PranaDB:

* Is not an in-memory a cache. It is designed from the beginning to be a true horizontally scalable distributed database
  and support very large persistent views.
* Uses a standard SQL "tables only" mental model that is familiar to anyone who has used a relational database.
* Once ingested, Prana owns the data. It does not delegate internal intermediate storage to an event streaming platform,
  such as Apache Kafka.

## What language is PranaDB written in?

Go

# Why the name PranaDB?

It comes from the Sanskrit word [Prana](https://en.wikipedia.org/wiki/Prana)
