from timeit import default_timer as timer
import datetime
import dateutil.parser
import getopt
import matplotlib.pyplot as plt
import pygal
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/..")
sys.path.append(os.path.dirname(os.path.abspath(__file__))+"/.")
from mupifDB import restApiControl

# for small stat use plain matplotlib
plt.switch_backend('agg')

# directory where to save generated figs
imageDir = os.path.dirname(os.path.abspath(__file__))+"/../webapi/static/images/"


def getHourlyExecutionStat(nrec=48):
    # for last nrec hours
    hourlyScheduledExecutions = [0]*nrec
    hourlyFinishedExecutions = [0]*nrec
    # get the current date
    now = datetime.datetime.now()
    nowh1 = now+datetime.timedelta(hours=1, minutes=-now.minute, seconds=-now.second, microseconds=-now.microsecond) 
    for wed in restApiControl.getExecutionRecords():
        scheduledDate = None
        if 'ScheduledDate' in wed.keys():
            scheduledDate = wed['ScheduledDate']
        elif 'StartDate' in wed.keys():
            scheduledDate = wed['StartDate']
        if scheduledDate:
            if isinstance(scheduledDate, str):
                scheduledDate = dateutil.parser.parse(scheduledDate)
            # print("Scheduled:"+str(scheduledDate))
            # get difference in hours
            diff = int((nowh1-scheduledDate).total_seconds()//3600)
            if diff < nrec:
                # print(diff)
                hourlyScheduledExecutions[nrec-1-diff] += 1
        if 'EndDate' in wed.keys():
            finishedDate = wed['EndDate']
            if finishedDate:
                if isinstance(finishedDate, str):
                    finishedDate = dateutil.parser.parse(finishedDate)
                # print('finishedDate:'+str(finishedDate))
                # get difference in hours
                diff = int((nowh1-finishedDate).total_seconds() // 3600)
                # print("Monday2:",monday2, diff)
                if diff < nrec:
                    hourlyFinishedExecutions[nrec-1-diff] += 1
    xlabels = []
    startDateTime = nowh1-datetime.timedelta(hours=nrec)
    for hr in range(nrec):
        xlabels.append((startDateTime+datetime.timedelta(hours=hr)).hour)
    return {'ScheduledExecutions': hourlyScheduledExecutions, 'ProcessedExecutions': hourlyFinishedExecutions, 'xlabels': xlabels}


def getWeeklyExecutionStat():
    weeklyScheduledExecutions = [0]*52
    weeklyFinishedExecutions = [0]*52
    # get the current date
    today = datetime.date.today()
    monday = (today - datetime.timedelta(days=today.weekday()))
    # print("today:"+str(today)+" monday:"+str(monday))
    for wed in restApiControl.getExecutionRecords():
        scheduledDate = None
        if 'ScheduledDate' in wed.keys():
            scheduledDate = wed['ScheduledDate']
        elif 'StartDate' in wed.keys():
            scheduledDate = wed['StartDate']
        if scheduledDate:
            if isinstance(scheduledDate, str):
                scheduledDate = dateutil.parser.parse(scheduledDate).date()
            # print("Scheduled:"+str(scheduledDate))
            # get difference in weeks
            monday2 = (scheduledDate - datetime.timedelta(days=scheduledDate.weekday()))
            # print("Monday2:", monday2, (monday-monday2).days)
            diff = (monday - monday2).days // 7
            # print(monday2, diff)
            if diff < 52:
                weeklyScheduledExecutions[51-diff] += 1
        if 'EndDate' in wed.keys():
            finishedDate = wed['EndDate']
            if finishedDate:
                if isinstance(finishedDate, str):
                    finishedDate = dateutil.parser.parse(finishedDate).date()
                # print('finishedDate:'+str(finishedDate))
                # get difference in weeks
                monday2 = (scheduledDate - datetime.timedelta(days=scheduledDate.weekday()))
                diff = (monday - monday2).days // 7
                # print("Monday2:",monday2, diff)
                if diff < 52:
                    weeklyFinishedExecutions[51-diff] += 1
    return {'ScheduledExecutions': weeklyScheduledExecutions, 'ProcessedExecutions': weeklyFinishedExecutions}


def getMonthlyExecutionStat():
    return


def getGlobalStat():
    totalExecutions = 0
    finishedExecutions = 0
    failedExecutions = 0
    createdExecutions = 0
    pendingExecutions = 0
    scheduledExecutions = 0
    runningExecutions = 0
    for wed in restApiControl.getExecutionRecords():
        status = wed['Status']
        totalExecutions += 1
        if status == 'Finished':
            finishedExecutions += 1
        elif status == 'Failed':
            failedExecutions += 1
        elif status == 'Created':
            createdExecutions += 1
        elif status == 'Pending':
            pendingExecutions += 1
        elif status == 'Scheduled':
            scheduledExecutions += 1
        elif status == 'Running':
            runningExecutions += 1
    return {
        'totalExecutions': totalExecutions,
        'finishedExecutions': finishedExecutions,
        'failedExecutions': failedExecutions,
        'createdExecutions': createdExecutions,
        'pendingExecutions': pendingExecutions,
        'scheduledExecutions': scheduledExecutions,
        'runningExecutions': runningExecutions
    }


def usage():
    print("schedulerstat [-w] [-h]")
    print("  -w generates weekly (last 52 weeks) statistics")
    print("  -h generates hourly statistics")


if __name__ == '__main__':

    try:
        opts, args = getopt.getopt(sys.argv[1:], "wh")
    except getopt.GetoptError as err:
        # print help information and exit:
        usage()
        sys.exit(2)

    weekly = False
    hourly = False
    # override by commandline setting, if provided
    for o, a in opts:
        if o in "-w":
            weekly = True
        elif o in "-h":
            hourly = True

    start = timer()
    if weekly:
        ws = getWeeklyExecutionStat()
        line_chart = pygal.Bar(width=800, height=300, explicit_size=True, legend_at_bottom=True)
        line_chart.title = 'MupifDB Scheduler Usage Weekly Statistics'
        # line_chart.x_labels = map(str, range(2002, 2013))
        for label, data in ws.items():
            line_chart.add(label, data)
        line_chart.render_to_file(imageDir+"scheduler_weekly_stat.svg")
    if hourly:
        ws = getHourlyExecutionStat()
        line_chart = pygal.Bar(width=800, height=300, explicit_size=True, legend_at_bottom=True)
        line_chart.title = 'MupifDB Scheduler Usage Hourly Statistics (last 48 hrs)'
        line_chart.x_labels = ws['xlabels']
        line_chart.add('ScheduledExecutions', ws['ScheduledExecutions'])
        line_chart.add('ProcessedExecutions', ws['ProcessedExecutions'])
        line_chart.render_to_file(imageDir+"scheduler_hourly_stat.svg")    
        # generate small 24 hrs stat badge
        ws = getHourlyExecutionStat(nrec=48)
        xl = ws['xlabels']
        xt = []
        xtl = []
        for i in range(len(xl)):
            if xl[i] % 6 == 0:
                xt.append(i)
                xtl.append(xl[i])
        print(ws)
        print(xt)
        print(xtl)
        plt.figure(figsize=(1.5, 0.5))
        plt.bar(ws['xlabels'], ws['ProcessedExecutions'])
        plt.yticks([])
        plt.xticks(xt, xtl, fontsize="4")
        # plt.box(False)
        # with open (imageDir+"scheduler_hourly_stat.svg", "w") as img:
        plt.savefig(imageDir+"scheduler_hourly_stat_small.svg", format='svg', transparent=True, bbox_inches='tight')
        # clip off the xml headers from the image
        # svg_img = '<svg' + img.getvalue().split('<svg')[1]
        # return svg_img
        # with open (imageDir+"scheduler_hourly_stat.png","w") as img:
        plt.savefig(imageDir+"scheduler_hourly_stat_small.png", format='png', transparent=False, bbox_inches='tight')

    print(getGlobalStat())
    end = timer()
    print('getGlobalStat took %s' % (end-start))

 
