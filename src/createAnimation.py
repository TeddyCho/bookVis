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
    return(theCLOBFrameInfos, theFBAFrameInfos)
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
def animate(t):
    myIndex = int(t*myFPS)
    myCLOBFrameInfo = myCLOBFrameInfos[myIndex]
    myFrameEndTime = myCLOBFrameInfo.endTime
    myFBAFrameInfo = next(i for i in myFBAFrameInfos[::-1] if i.endTime <= myFrameEndTime)
    myTimeStamp = myCLOBFrameInfo.endTime
    
    ax0.cla()
    ax0.set_xlim(-5, 5)
    ax0.set_ylim(myPriceMin, myPriceMax)
    ax0.set_title("CLOB", color="white", alpha = .35)
    ax0.yaxis.tick_right()
    plt.sca(ax0)
    plt.yticks(color="white", alpha = myAxisAlpha)
    if myCLOBFrameInfo.bid != 0:
        myBidPrice = myCLOBFrameInfo.bid
        myBidSize = myCLOBFrameInfo.bidSize
        myBidCoord = [[(-myBidSize / 2, myBidPrice), (myBidSize / 2, myBidPrice)]]
        myCLOBBidBar.set_segments(myBidCoord)
    if myCLOBFrameInfo.offer != 0: 
        myOfferPrice = myCLOBFrameInfo.offer
        myOfferSize = myCLOBFrameInfo.offerSize
        myOfferCoord = [[(-myOfferSize / 2, myOfferPrice), (myOfferSize / 2, myOfferPrice)]]
        myCLOBOfferBar.set_segments(myOfferCoord)
    ax0.add_collection(myCLOBBidBar)
    ax0.add_collection(myCLOBOfferBar)
    
    for myFrameInfo in myCLOBFrameInfos:
        myFramesPast = (myFrameEndTime - myFrameInfo.endTime) / myTimeBucketDiff
        if myFramesPast >= 0:
            myFadeFactor = .90 ** float(myFramesPast)
            if myFadeFactor < .1:
                myFadeFactor = 0
            for myTrade in myFrameInfo.trades:
                random.seed(myTrade.price)
                myRandomXPos = random.gauss(0, 1.25)
                ax0.plot(myRandomXPos, myTrade.price, color = myTradeColor, marker = 'x',
                         alpha=.4 * myFadeFactor, markersize = myTrade.tradeVolume/10)
                ax0.plot(myRandomXPos, myTrade.price, color = myTradeColor, marker = '.',
                         alpha=.4 * myFadeFactor, markersize = myTrade.tradeVolume/10)
        
    global myFBALastEndTime
    global myIsMatched
    global myTradeTime
    global myMatchPrice
    global myMatchSize
    global myFBATrades
    ax1.cla()
    ax1.set_title("FBA", color="white", alpha = .35)
    ax1.set_xlim(-5, 5)
    ax1.set_ylim(myPriceMin, myPriceMax)
    if myFBAFrameInfo.endTime == myFBALastEndTime:
        myFBAMatchBar.set_alpha(float(myFBAMatchBar.get_alpha()) * .5)
        myFBABidBar.set_alpha(float(myFBABidBar.get_alpha()) * .5)
        myFBAOfferBar.set_alpha(float(myFBAOfferBar.get_alpha()) * .5)
    else:
        myRandDet = random.uniform(0, 1)
        myIsMatched = myRandDet > .4
        myFBAMatchBar.set_alpha(float(myFBAMatchBar.get_alpha()) * .5)
        myFBABidBar.set_alpha(float(myFBABidBar.get_alpha()) * .5)
        myFBAOfferBar.set_alpha(float(myFBAOfferBar.get_alpha()) * .5)
        myBidPrice = myFBAFrameInfo.bid
        myBidSize = myFBAFrameInfo.bidSize
        myOfferPrice = myFBAFrameInfo.offer
        myOfferSize = myFBAFrameInfo.offerSize
        if myIsMatched:
            myMatchPrice = (myBidPrice + myOfferPrice) / 2
            myMatchCoord = [[(1 / 2, myMatchPrice), (-1 / 2, myMatchPrice)]]
            myFBAMatchBar.set_segments(myMatchCoord)
            myFBAMatchBar.set_alpha(1)
            myMatchSize = random.gauss(10, 2)
            myTradeTime = myFBAFrameInfo.endTime
            myFBATrades.append({"Price":myMatchPrice, "Volume":myMatchSize, "DateTime":myTradeTime})
        else:
            myFBABidBar.set_alpha(1)
            myFBAOfferBar.set_alpha(1)
            if myFBAFrameInfo.bid != 0:
                myBidCoord = [[(-myBidSize / 2, myBidPrice), (myBidSize / 2, myBidPrice)]]
                myFBABidBar.set_segments(myBidCoord)
            if myFBAFrameInfo.offer != 0: 
                myOfferCoord = [[(-myOfferSize / 2, myOfferPrice), (myOfferSize / 2, myOfferPrice)]]
                myFBAOfferBar.set_segments(myOfferCoord)
        for i in myFBATrades:
            myTradeTime = i["DateTime"]
            myMatchPrice = i["Price"]
            myMatchSize = i["Volume"]
            myTradeFramesPast = (myFrameEndTime - myTradeTime) / myTimeBucketDiff
            myFadeFactor = .7 ** float(myTradeFramesPast)
            ax1.plot(0, myMatchPrice, color = myTradeColor, marker = 'x',
                     alpha=.9 * myFadeFactor, markersize = myMatchSize/1)
            ax1.plot(0, myMatchPrice, color = myTradeColor, marker = '.',
                     alpha=.9 * myFadeFactor, markersize = myMatchSize/1)
        myFBALastEndTime = myFBAFrameInfo.endTime
        ax1.add_collection(myFBAMatchBar)
        ax1.add_collection(myFBABidBar)
        ax1.add_collection(myFBAOfferBar)
    
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
    myStartTime = datetime.datetime.strptime("20140709 09:30:00", "%Y%m%d %H:%M:%S")
    myEndTime = datetime.datetime.strptime("20140709 11:30:00", "%Y%m%d %H:%M:%S")
    myGifDuration, myFPS = 10, 20
    myPriceMin, myPriceMax = 568, 576
    myFBAInterval = datetime.timedelta(seconds=5)
    myFBALastEndTime = asDateTime(0)
    myOutputFolder = os.path.join(os.getcwd(), "..\\output\\framesGOOGOneSec\\")
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
    myCLOBBidBar = mc.LineCollection([[(0, 0), (0, 0)]], linewidths=2, color = myBidColor)
    myCLOBOfferBar = mc.LineCollection([[(0, 0), (0, 0)]], linewidths=2, color = myOfferColor)
    ax0.add_collection(myCLOBBidBar)
    ax0.add_collection(myCLOBOfferBar)
    
    ax1 = initializeSnapshotGraph(myPriceMin, myPriceMax, gs[:-2, 1], "FBA")
    ax1.spines['right'].set_alpha(0)
    myFBABidBar = mc.LineCollection([[(0, 0), (0, 0)]], linewidths=2, color = myBidColor, alpha = 1)
    myFBAOfferBar = mc.LineCollection([[(0, 0), (0, 0)]], linewidths=2, color = myOfferColor, alpha = 1)
    myFBAMatchBar = mc.LineCollection([[(0, 0), (0, 0)]], linewidths=2, color = "green", alpha = 0)
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