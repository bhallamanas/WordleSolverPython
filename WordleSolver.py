from pyspark.sql import SparkSession
from pyspark.sql.functions import length
from pyspark.sql.functions import rand
from pyspark.sql.functions import col
import re

spark = SparkSession.builder.master("local[*]").getOrCreate()


def openFile(path):
    currentDB = spark.read.text(path)
    #currentDB.show()
    return currentDB


def filter(currentDB):
    filterDF = currentDB.filter(length("value") == 5)
    #filterDF.show()
    return filterDF


def userInput(dF):
    guess = input("Hi, please enter your guess: ")
    notFive = True
    while notFive:
        if len(guess) == 5:
            #realWord(guess, dF)
            notFive = False
        else:
            guess = input("The guess must be 5 letters, please try again: ")

    return guess


def pickRandom(df):
    random = df.select('value').orderBy(rand()).limit(1).collect()
    return random


def checker(guess, word, dF):
    missed = True


    while missed:
        splitGuess = split(guess)
        splitWord = split(word[0])
        fSplitWord = split(splitWord)

        #print(splitGuess)
        #print(fSplitWord)
        #print(type(fSplitWord))
        if splitGuess == fSplitWord:
            print("Your guess was correct.")
            missed = False
        else:
            print("Your guess was incorrect, please try again")
            result = positionChecker(fSplitWord, splitGuess)

            helpRequested = True
            while helpRequested:
                help = input("Would you like some help: (Y/N): ")
                if help == "N":
                    helpRequested = False
                elif help == "Y":
                    dF = remainingWords(splitGuess, result, dF)
                    helpRequested = False
            guess = userInput(dF)



def split(word):
    return [char for char in word]

def positionChecker(word, guess):
    index = 0
    result = ['N', 'N', 'N', 'N', 'N']
    for char in word:
        if word[index] == guess[index]:
            result[index] = 'G'
        else:
            for char in word:
                if char == guess[index]:
                    result[index] = 'Y'
        index = index + 1

    print(result)
    return(result)

def realWord(guess, dF):
    dF.filter(col("value").contains(guess)).show()

def remainingWords(guess, result, dF):
    #print("entering function")
    index = 0
    for char in result:
        if char == 'Y':
            letter = guess[index]
            dF = dF.filter(col("value").contains(letter))
        elif char == 'G':
            letter = guess[index]
            dF = dF.filter(col("value").contains(letter))
        elif char == 'N':
            letter = guess[index]
            dF = dF.filter(~col("value").contains(letter))
        index = index + 1
    dF.show(58000)
    return dF


    #dF.filter(col("value").contains('S')).show()



currentDF = openFile("./wordleDB.txt")
filteredDF = filter(currentDF)
randVal = pickRandom(filteredDF)
word = randVal[0]
#print(type(word))

#print(word)
guess = userInput(filteredDF)
checker(guess, word, filteredDF)

#remainingWords(guess, filteredDF)

