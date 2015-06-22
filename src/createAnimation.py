import os
import zipfile
import csv
import datetime
import numpy as np
import matplotlib.pyplot as plt
import random
from matplotlib import collections as mc
from matplotlib import gridspec
import matplotlib.dates as mdates
from moviepy.video.io.bindings import mplfig_to_npimage
import moviepy.editor as mpy
from matplotlib.dates import date2num
from PIL import Image

class FrameInfo:
    def __init__(self, aStartTime, aTimeBucketDiff):
        self.startTime = aStartTime
        self.endTime = aStartTime + aTimeBucketDiff
        self.orderBooks = list()
        self.trades = list()
        self.bid = 0
        self.offer = 0
        self.bidSize = 0
        self.offerSize = 0
        self.matchPrice = 0
        self.matchSize = 0
    def addOrder(self, aOrderBook):
        self.orderBooks.append(aOrderBook)
        self.bid = aOrderBook.bid
        self.bidSize = aOrderBook.bidSize
        self.offer = aOrderBook.offer
        self.offerSize = aOrderBook.offerSize
    def addTrade(self, aTrade):
        self.trades.append(aTrade)
class OrderBook:
    def __init__(self, aRow):
        self.dateTime = datetime.datetime.strptime(aRow["DATE"] + " " + aRow["TIME"], "%Y%m%d %H:%M:%S")
        self.symbol = aRow["SYMBOL"]
        self.exchange = inferExchange(aRow["EX"])
        self.bid = float(aRow["BID"])
        self.bidSize = int(aRow["BIDSIZ"])
        self.offer = float(aRow["OFR"])
        self.offerSize = int(aRow["OFRSIZ"])
        self.mode = aRow["MODE"]
        self.mmId = aRow["MMID"]
class Trade:
    def __init__(self, aRow):
        self.symbol = aRow["SYMBOL"]
        self.dateTime = datetime.datetime.strptime(aRow["DATE"] + " " + aRow["TIME"], "%Y%m%d %H:%M:%S")
        self.price = float(aRow["PRICE"])
        self.tradeVolume = int(aRow["SIZE"])
        self.saleCondition = aRow["COND"]
        self.exchange = inferExchange(aRow["EX"])
def unzipFile(aFileName):
    myFilePath = os.path.join(os.getcwd(), "..") + '\\data\\' + aFileName + '_csv.zip'
    print("Unzipping " + myFilePath + "...")
    myFile = open(myFilePath, 'rb')
    z = zipfile.ZipFile(myFile)
    for name in z.namelist():
        z.extract(name, os.path.join(os.getcwd(), "..") + "\\data\\")
        myFile.close()
    return(os.path.join(os.getcwd(), "..") + "\\data\\" + aFileName + ".csv")
def inferExchange(aExchangeString):
    myCodeDict = {"A":"NYSE MKT", "B":"NASDAQ OMX BX", "C":"National",
                  "D":"FINRA", "I":"ISE", "J":"Direct Edge A",
                  "K":"Direct Edge X", "M":"Chicago", "N":"NYSE",
                  "T":"NASDAQ OMX", "P":"NYSE Arca SM", "S":"Consolidated Tape System",
                  "T/Q":"NASDAQ", "Q":"NASDAQ", "W":"CBOE", "X":"NASDAQ OMX PSX",
                  "Y":"BATS Y", "Z":"BATS"}
    try:
        return myCodeDict[aExchangeString]
    except:
        return "Invalid Exchange"
def asSeconds(aDateTime):
    return(aDateTime - datetime.datetime(1970, 1, 1)).total_seconds()
def asDateTime(aSeconds):
    return(datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=aSeconds))
def getFrameInfo(aOrderBuffer):
    myLastRow = aOrderBuffer.pop()
    return(myLastRow)
def fastForwardReaderPastDateTime(aReader, aDateTime):
    myCurrentRow = next(aReader)
    while datetime.datetime.strptime(myCurrentRow["DATE"] + " " + myCurrentRow["TIME"],
                                     "%Y%m%d %H:%M:%S") <= myStartTime:
        myCurrentRow = next(aReader)
    return(myCurrentRow)
def carryValuesThroughZeros(aList):
    myLastValue = 9e9
    for i in range(len(aList)):
        myValue = aList[i]
        if myValue == 0:
            aList[i] = myLastValue
        else:
            myLastValue = myValue
    return(aList)
def determineFBAState(aFBAInfoInterval):            
    theFBAStateInterval = aFBAInfoInterval
    if random.uniform(0,1) < .4:
        myMatchPrice = (theFBAStateInterval.bid + theFBAStateInterval.offer) / 2
        theFBAStateInterval.matchPrice = myMatchPrice
        theFBAStateInterval.matchSize = 2
    return(theFBAStateInterval)
def FBAInfoIntervalsToFrameInfos(aFBAInfoIntervals, aStartTime, aEndTime, aTimeBucketDiff):
    theFBAFrameInfos = [FrameInfo(asDateTime(st), aTimeBucketDiff) for st in range(int(asSeconds(aStartTime)),
                                                                   int(asSeconds(aEndTime)),
                                                                   int(aTimeBucketDiff.total_seconds()))]
    
    for i in range(len(theFBAFrameInfos)):
        myEndTime = theFBAFrameInfos[i].endTime
        myStartTime = theFBAFrameInfos[i].startTime
        myLatestFBAInfoInterval = determineFBAState(next(i for i in aFBAInfoIntervals[::-1] if i.endTime <= myEndTime))
        """input(myLatestFBAInfoInterval.bid)
        input(myLatestFBAInfoInterval.startTime)
        input(myStartTime)"""
        if myLatestFBAInfoInterval.endTime > myStartTime:
            theFBAFrameInfos[i].orderBooks = myLatestFBAInfoInterval.orderBooks
            theFBAFrameInfos[i].trades = myLatestFBAInfoInterval.trades
            theFBAFrameInfos[i].bid = myLatestFBAInfoInterval.bid
            theFBAFrameInfos[i].offer = myLatestFBAInfoInterval.offer
            theFBAFrameInfos[i].matchPrice = myLatestFBAInfoInterval.matchPrice
            theFBAFrameInfos[i].bidSize = myLatestFBAInfoInterval.bidSize
            theFBAFrameInfos[i].offerSize = myLatestFBAInfoInterval.offerSize
            theFBAFrameInfos[i].matchSize = myLatestFBAInfoInterval.matchSize
    return(theFBAFrameInfos)
def checkOutBookFile(aOrderReader, aTradeReader, aStartTime, aEndTime, aTimeBucketDiff, aFBAInterval):
    theCLOBFrameInfos = [FrameInfo(asDateTime(st), aTimeBucketDiff) for st in range(int(asSeconds(aStartTime)),
                                                                   int(asSeconds(aEndTime)),
                                                                   int(aTimeBucketDiff.total_seconds()))]
    theFBAFrameInfos = [FrameInfo(asDateTime(st), aTimeBucketDiff) for st in range(int(asSeconds(aStartTime)),
                                                                   int(asSeconds(aEndTime)),
                                                                   int(aFBAInterval.total_seconds()))]
    myExchange = "NASDAQ OMX BX"
    for myRow in aOrderReader:
        myRowDateTime = datetime.datetime.strptime(myRow["DATE"] + " " + myRow["TIME"], "%Y%m%d %H:%M:%S")
        myRowExchange = inferExchange(myRow["EX"])
        if(myRowDateTime >= aStartTime and myRowExchange == myExchange):
            myCurrentOrderBook = OrderBook(myRow)
            for myCurrentFrameInfo in theCLOBFrameInfos:
                if(myCurrentFrameInfo.startTime < myRowDateTime and myRowDateTime <= myCurrentFrameInfo.endTime):
                    myCurrentFrameInfo.addOrder(myCurrentOrderBook)
            for myCurrentFrameInfo in theFBAFrameInfos:
                if(myCurrentFrameInfo.startTime < myRowDateTime and myRowDateTime <= myCurrentFrameInfo.endTime):
                    myCurrentFrameInfo.addOrder(myCurrentOrderBook)
    for myRow in aTradeReader:
        myRowDateTime = datetime.datetime.strptime(myRow["DATE"] + " " + myRow["TIME"], "%Y%m%d %H:%M:%S")
        myRowExchange = inferExchange(myRow["EX"])
        if(myRowDateTime >= aStartTime and myRowExchange == myExchange):
            myCurrentTrade = Trade(myRow)
            for myCurrentFrameInfo in theCLOBFrameInfos:
                if(myCurrentFrameInfo.startTime < myRowDateTime and myRowDateTime <= myCurrentFrameInfo.endTime):
                    myCurrentFrameInfo.addTrade(myCurrentTrade)
            for myCurrentFrameInfo in theFBAFrameInfos:
                if(myCurrentFrameInfo.startTime < myRowDateTime and myRowDateTime <= myCurrentFrameInfo.endTime):
                    myCurrentFrameInfo.addTrade(myCurrentTrade)
    return(theCLOBFrameInfos, FBAInfoIntervalsToFrameInfos(theFBAFrameInfos, aStartTime, aEndTime, aTimeBucketDiff))
def initializeSnapshotGraph(aPriceMin, aPriceMax, aGSLocation, aTitle):
    ax = plt.subplot(aGSLocation)
    ax.set_xlim(-5, 5)
    ax.set_ylim(aPriceMin, aPriceMax)
    ax.set_axis_bgcolor('black')
    ax.spines['bottom'].set_color('white')
    ax.spines['bottom'].set_alpha(myAxisAlpha)
    ax.spines['left'].set_color('white')
    ax.spines['left'].set_alpha(myAxisAlpha)
    ax.spines['right'].set_color('white')
    ax.spines['right'].set_alpha(myAxisAlpha)
    plt.yticks(color="white", alpha = myAxisAlpha)
    ax.set_title(aTitle, color="white", alpha = .35)
    plt.ylabel("Price ($)", color="white", alpha = myAxisAlpha)
    return ax
def initializeTimeGraph(aPriceMin, aPriceMax, aStartTime, aTimeEnd, aGSLocation):
    ax = plt.subplot(aGSLocation)
    ax.set_ylim(aPriceMin, aPriceMax)
    ax.set_xlim([aStartTime, aTimeEnd])
    plt.xticks(rotation=70, color="white", alpha = myAxisAlpha)
    plt.yticks([myPriceMin, myPriceMax], color="white")
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.yticks(color="white", alpha = myAxisAlpha)
    ax.plot(myTimeStamps, myBids, color = myBidColor)
    ax.plot(myTimeStamps, myOffers, color = myOfferColor)
    plt.gcf().autofmt_xdate()
    ax.set_axis_bgcolor('black')
    ax.spines['left'].set_color('white')
    ax.spines['left'].set_alpha(myAxisAlpha)
    ax.spines['bottom'].set_color('white')
    ax.spines['bottom'].set_alpha(myAxisAlpha)
    ax.spines['right'].set_color('white')
    ax.spines['right'].set_alpha(myAxisAlpha)
    return ax
def updateQuoteBar(aQuotePrice, aQuoteSize, aQuoteBar):
    if aQuotePrice == 0:
        aQuoteBar.set_alpha(aQuoteBar.get_alpha()*.8)
    else:
        myBidPrice = aQuotePrice
        myBidSize = aQuoteSize
        myBidCoord = [[(-myBidSize / 2, myBidPrice), (myBidSize / 2, myBidPrice)]]
        aQuoteBar.set_segments(myBidCoord)
        aQuoteBar.set_alpha(1)
    return(aQuoteBar)
    
def drawTradeInfoOnAxis(aPlt, aAxis, aFrameEndTime, aFrameInfo, myPastFrameInfos, myTimeBucketDiff, aBidBar, aOfferBar, aFBAMatchBar,
                        aYMin, aYMax, aAxisAlpha, aIsCLOB):
    aAxis.cla()
    aAxis.set_xlim(-5, 5)
    aAxis.set_ylim(aYMin, aYMax)
    aAxis.set_title(("CLOB" if aIsCLOB else "FBA"), color="white", alpha = .35)
    aAxis.yaxis.tick_right()
    aPlt.sca(aAxis)
    aPlt.yticks(color="white", alpha = aAxisAlpha)
    
    if aFrameInfo.matchPrice != 0:
        aFrameInfo.bid = 0
        aFrameInfo.offer = 0
        myFBATrades.append({"Price":aFrameInfo.matchPrice, "Volume":aFrameInfo.matchSize, "DateTime":aFrameInfo.endTime, "Alpha":.6})
    
    aFBAMatchBar = updateQuoteBar(aFrameInfo.matchPrice, aFrameInfo.matchSize, aFBAMatchBar)
    aBidBar = updateQuoteBar(aFrameInfo.bid, aFrameInfo.bidSize, aBidBar)
    aOfferBar = updateQuoteBar(aFrameInfo.offer, aFrameInfo.offerSize, aOfferBar)
    
    aAxis.add_collection(aBidBar)
    aAxis.add_collection(aOfferBar)
    aAxis.add_collection(aFBAMatchBar)
    
    if aIsCLOB:
        for myFrameInfo in myPastFrameInfos:
            myFramesPast = (aFrameEndTime - myFrameInfo.endTime) / myTimeBucketDiff
            if myFramesPast >= 0:
                myFadeFactor = .90 ** float(myFramesPast)
                if not myFadeFactor < .1:
                    for myTrade in myFrameInfo.trades:
                        random.seed(myTrade.price)
                        myRandomXPos = random.gauss(0, 1.25)
                        aAxis.plot(myRandomXPos, myTrade.price, color = myTradeColor, marker = 'x',
                                 alpha=.4 * myFadeFactor, markersize = myTrade.tradeVolume/10)
                        aAxis.plot(myRandomXPos, myTrade.price, color = myTradeColor, marker = '.',
                                 alpha=.4 * myFadeFactor, markersize = myTrade.tradeVolume/10)
    else:
        for i in myFBATrades:
            myMatchPrice = i["Price"]
            myMatchSize = i["Volume"]
            ax1.plot(0, myMatchPrice, color = myTradeColor, marker = 'x',
                     alpha = i["Alpha"], markersize = myMatchSize*10)
            ax1.plot(0, myMatchPrice, color = myTradeColor, marker = '.',
                     alpha = i["Alpha"], markersize = myMatchSize*10)
            i["Alpha"] = i["Alpha"] * .6

def animate(t):
    myIndex = int(t*myFPS)
    myCLOBFrameInfo = myCLOBFrameInfos[myIndex]
    myFrameEndTime = myCLOBFrameInfo.endTime
    myFBAFrameInfo = myFBAFrameInfos[myIndex]
    # myFBAFrameInfo = next(i for i in myFBAFrameInfos[::-1] if i.endTime <= myFrameEndTime)
    myTimeStamp = myCLOBFrameInfo.endTime
    
    drawTradeInfoOnAxis(plt, ax0, myFrameEndTime, myCLOBFrameInfo, myCLOBFrameInfos, myTimeBucketDiff, myCLOBBidBar, myCLOBOfferBar, myFBAMatchBar,
                        myPriceMin, myPriceMax, myAxisAlpha, True) # put in fba match bar in there
    
    drawTradeInfoOnAxis(plt, ax1, myFrameEndTime, myFBAFrameInfo, myFBAFrameInfos, myTimeBucketDiff, myFBABidBar, myFBAOfferBar, myFBAMatchBar,
                        myPriceMin, myPriceMax, myAxisAlpha, False)
    
    global myFBALastEndTime
    global myIsMatched
    global myTradeTime
    global myMatchPrice
    global myMatchSize
    global myFBATrades
    
    myBidTracker.set_xdata([myTimeStamps[myIndex]])
    myBidTracker.set_ydata([myBids[myIndex]])
    myOfferTracker.set_xdata([myTimeStamps[myIndex]])
    myOfferTracker.set_ydata([myOffers[myIndex]])
    for myTrade in myCLOBFrameInfo.trades:
        ax2.plot(myTimeStamps[myIndex], myTrade.price, color = myTradeColor, marker = 'x', alpha = .4, markersize = myTrade.tradeVolume/10)
        ax2.plot(myTimeStamps[myIndex], myTrade.price, color = myTradeColor, marker = '.', alpha = .4, markersize = myTrade.tradeVolume/20)
    fig.canvas.draw()
    ax2.set_title(myTimeStamp.strftime("%d/%m/%y %H:%M:%S"), color="white", alpha = .35)    
    return(mplfig_to_npimage(fig))

def extractFrames(inGif, outFolder):
    frame = Image.open(outFolder + inGif + '.gif')
    nframes = 0
    while frame:
        frame.save( '%s%s%s.gif' % (outFolder, inGif, nframes,  ), 'GIF')
        nframes += 1
        try:
            frame.seek( nframes )
        except EOFError:
            break;
    return True
if __name__ == '__main__':
    myOrderCsvFileName, myTradeCsvFileName = unzipFile("507b7597f38788bd"), unzipFile("ec166543c332e071")
    myStartTime = datetime.datetime.strptime("20140709 08:30:00", "%Y%m%d %H:%M:%S")
    myEndTime = datetime.datetime.strptime("20140709 10:30:00", "%Y%m%d %H:%M:%S")
    myGifDuration, myFPS = 10, 20
    myPriceMin, myPriceMax = 568, 576
    myFBAInterval = datetime.timedelta(seconds=360)
    myFBALastEndTime = asDateTime(0)
    myOutputFolder = os.path.join(os.getcwd(), "..\\output\\framesGOOG360Sec\\")
    myGifFileName = "frame"
    
    myBidColor, myOfferColor, myTradeColor = "#008CBA", "#FF3333", "#FFCC33"
    myAxisAlpha = .25
    myIsMatched = False
    myTradeTime = asDateTime(0)
    myMatchPrice = 0
    myMatchSize = 0
    
    myFBATrades = list()
    
    myTimeRange = myEndTime - myStartTime
    myFrameCount = myGifDuration * myFPS + 1
    myTimeBucketDiff = myTimeRange / myFrameCount
    with open(myOrderCsvFileName, 'r') as orderFile, open(myTradeCsvFileName, "r") as tradeFile: 
        myOrderReader = csv.DictReader(orderFile)
        myTradeReader = csv.DictReader(tradeFile)
        myCLOBFrameInfos, myFBAFrameInfos = checkOutBookFile(myOrderReader, myTradeReader, myStartTime,
                                                             myEndTime, myTimeBucketDiff, myFBAInterval)
    fig = plt.figure(facecolor='black', figsize=(6,6), dpi=160)
    #fig.suptitle("Price ($)", color = "white", alpha = .35)
    gs = gridspec.GridSpec(6, 2)
    
    ax0 = initializeSnapshotGraph(myPriceMin, myPriceMax, gs[:-2, 0], "CLOB")
    ax0.spines['left'].set_alpha(0)
    myCLOBBidBar = mc.LineCollection([[(0, 0), (0, 0)]], linewidths=2, color = myBidColor, alpha = 1)
    myCLOBOfferBar = mc.LineCollection([[(0, 0), (0, 0)]], linewidths=2, color = myOfferColor, alpha = 1)
    ax0.add_collection(myCLOBBidBar)
    ax0.add_collection(myCLOBOfferBar)
    
    ax1 = initializeSnapshotGraph(myPriceMin, myPriceMax, gs[:-2, 1], "FBA")
    ax1.spines['right'].set_alpha(0)
    myFBABidBar = mc.LineCollection([[(0, 0), (0, 0)]], linewidths=2, color = myBidColor, alpha = 1)
    myFBAOfferBar = mc.LineCollection([[(0, 0), (0, 0)]], linewidths=2, color = myOfferColor, alpha = 1)
    myFBAMatchBar = mc.LineCollection([[(0, 0), (0, 0)]], linewidths=2, color = "green", alpha = 1)
    ax1.add_collection(myFBABidBar)
    ax1.add_collection(myFBAOfferBar)
    ax1.add_collection(myFBAMatchBar)
    ax1.get_yaxis().set_visible(False)
    
    myBids = carryValuesThroughZeros([x.bid for x in myCLOBFrameInfos])
    myOffers = carryValuesThroughZeros([x.offer for x in myCLOBFrameInfos])
    myTimeStamps = [date2num(x.endTime) for x in myCLOBFrameInfos]
    ax2 = initializeTimeGraph(myPriceMin, myPriceMax, myStartTime, myEndTime, gs[-1, :])
    myBidTracker, = ax2.plot([myTimeStamps[0]], [myBids[0]], color = myBidColor, marker = "d", alpha = .6)
    myOfferTracker, = ax2.plot([myTimeStamps[0]], [myOffers[0]], color = myOfferColor, marker = "d", alpha = .6)
    
    animation = mpy.VideoClip(animate, duration=myGifDuration)
    if not os.path.exists(myOutputFolder):
        os.makedirs(myOutputFolder)
    animation.write_gif(myOutputFolder + myGifFileName + ".gif", fps=myFPS)
    extractFrames(myGifFileName, myOutputFolder)