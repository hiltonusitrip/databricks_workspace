# Databricks notebook source
book_stream = spark.readStream.table('books_stream');

display(books_stream)