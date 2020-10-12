from pymongo import MongoClient
from timeit import default_timer as timer
import datetime
import dateutil.parser
import pygal


def getHourlyExecutionStat(db):
    # for last 48 hours
    hourlyScheduledExecutions = [0]*48
    hourlyFinishedExecutions  = [0]*48
    #get the current date
    now = datetime.datetime.now()
    nowh1 = now+datetime.timedelta(hours=1, minutes=-now.minute, seconds=-now.second, microseconds=-now.microsecond) 
    for wed in db.WorkflowExecutions.find():
        ScheduledDate = None
        if ('ScheduledDate' in wed.keys()):
            scheduledDate = wed['ScheduledDate']
        elif ('StartDate' in wed.keys()):
            scheduledDate = wed['StartDate']
        if (scheduledDate):
            if isinstance(scheduledDate, str):
                scheduledDate = dateutil.parser.parse(scheduledDate)
            #print ("Scheduled:"+str(scheduledDate))
            # get difference in hours
            diff = int((nowh1-scheduledDate).total_seconds()//3600)
            if (diff < 48):
                print (diff)
                hourlyScheduledExecutions[47-diff]+=1
        if ('EndDate' in wed.keys()):
            finishedDate = wed['EndDate']
            if (finishedDate):
                if isinstance(finishedDate, str):
                    finishedDate = dateutil.parser.parse(finishedDate)
                #print ('finishedDate:'+str(finishedDate))
                # get difference in hours
                diff = int((nowh1-finishedDate).total_seconds() // 3600)
                #print("Monday2:",monday2, diff)
                if (diff < 48):
                    hourlyFinishedExecutions[47-diff]+=1
    xlabels=[]
    startDateTime = nowh1-datetime.timedelta(hours=48)
    for hr in range(48):
        xlabels.append((startDateTime+datetime.timedelta(hours=hr)).hour)
    return {'ScheduledExecutions':hourlyScheduledExecutions, 'ProcessedExecutions':hourlyFinishedExecutions, 'xlabels':xlabels}


def getWeeklyExecutionStat(db):
    weeklyScheduledExecutions = [0]*52
    weeklyFinishedExecutions  = [0]*52
    #get the current date
    today = datetime.date.today()
    monday = (today - datetime.timedelta(days=today.weekday()))
    #print("today:"+str(today)+" monday:"+str(monday))
    for wed in db.WorkflowExecutions.find():
        ScheduledDate = None
        if ('ScheduledDate' in wed.keys()):
            scheduledDate = wed['ScheduledDate']
        elif ('StartDate' in wed.keys()):
            scheduledDate = wed['StartDate']
        if (scheduledDate):
            if isinstance(scheduledDate, str):
                scheduledDate = dateutil.parser.parse(scheduledDate).date()
            #print ("Scheduled:"+str(scheduledDate))
            # get difference in weeks
            monday2 = (scheduledDate - datetime.timedelta(days=scheduledDate.weekday()))
            #print("Monday2:", monday2, (monday-monday2).days)
            diff = (monday - monday2).days // 7
            #print(monday2, diff)
            if (diff < 52):
                weeklyScheduledExecutions[51-diff]+=1
        if ('EndDate' in wed.keys()):
            finishedDate = wed['EndDate']
            if (finishedDate):
                if isinstance(finishedDate, str):                                                                                                                                                                              finishedDate = dateutil.parser.parse(finishedDate).date()
                #print ('finishedDate:'+str(finishedDate))
                # get difference in weeks
                monday2 = (scheduledDate - datetime.timedelta(days=scheduledDate.weekday()))
                diff = (monday - monday2).days // 7
                #print("Monday2:",monday2, diff)
                if (diff < 52):
                    weeklyFinishedExecutions[51-diff]+=1
    return {'ScheduledExecutions':weeklyScheduledExecutions, 'ProcessedExecutions':weeklyFinishedExecutions}


def getMonthlyExecutionStat(db):
    return

def getGlobalStat(db):
    totalExecutions = 0
    finishedExecutions = 0
    failedExecutions = 0
    createdExecutions = 0
    pendingExecutions = 0
    scheduledExecutions = 0
    runningExecutions = 0
    for wed in db.WorkflowExecutions.find():
        status = wed['Status']
        totalExecutions+=1
        if (status == 'Finished'):
            finishedExecutions+=1
        elif (status == 'Failed'):
            failedExecutions += 1
        elif (status == 'Created'):
            createdExecutions+=1
        elif (status == 'Pending'):
            pendingExecutions+=1
        elif (status == 'Scheduled'):
            scheduledExecutions+=1
        elif (status == 'Running'):
            runningExecutions+=1
    return {
        'totalExecutions': totalExecutions,
        'finishedExecutions': finishedExecutions,
        'failedExecutions':failedExecutions,
        'createdExecutions':createdExecutions,
        'pendingExecutions':pendingExecutions,
        'scheduledExecutions':scheduledExecutions,
        'runningExecutions':runningExecutions}


if __name__ == '__main__':
    client = MongoClient()                                                                                                                                                                                 
    db = client.MuPIF

    start = timer ()
    print (getGlobalStat(db))
    end=timer()
    print ('getGlobalStat took %s'%(end-start))

    start=timer()
    ws = getHourlyExecutionStat(db)
    print(ws)
    line_chart = pygal.Bar()
    line_chart.title = 'MupifDB Scheduler Usage Hourly Statistics'
    #line_chart.x_labels = map(str, range(2002, 2013))
    for label, data in ws.items():
        line_chart.add(label, data)
    line_chart.render()
    line_chart.render_to_file('hourly.svg')
    end=timer()                                                                                                                                                                                            
    print ('hourlyStat took %s'%(end-start))   
