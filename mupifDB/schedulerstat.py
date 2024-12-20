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
    # XXX: pass time range to the query
    for wed in restApiControl.getExecutionRecords():
        if wed.StartDate: # may be None
            # print("Scheduled:"+str(scheduledDate))
            # get difference in hours
            diff = int((nowh1-wed.StartDate).total_seconds()//3600)
            if diff < nrec:
                # print(diff)
                hourlyScheduledExecutions[nrec-1-diff] += 1
        if wed.EndDate: # may be None
            # print('finishedDate:'+str(finishedDate))
            # get difference in hours
            diff = int((nowh1-wed.EndDate).total_seconds() // 3600)
            # print("Monday2:",monday2, diff)
            if diff < nrec:
                hourlyFinishedExecutions[nrec-1-diff] += 1
    startDateTime = nowh1-datetime.timedelta(hours=nrec)
    return {'ScheduledExecutions': hourlyScheduledExecutions, 'ProcessedExecutions': hourlyFinishedExecutions, 'xlabels': [(startDateTime+datetime.timedelta(hours=hr)).hour for hr in range(nrec)]}


def getWeeklyExecutionStat():
    weeklyScheduledExecutions = [0]*52
    weeklyFinishedExecutions = [0]*52
    # get the current date
    today = datetime.date.today()
    monday = (today - datetime.timedelta(days=today.weekday()))
    # XXX: pass time range to the query
    for wed in restApiControl.getExecutionRecords():
        if wed.StartDate:
            # print("Scheduled:"+str(scheduledDate))
            # get difference in weeks
            monday2 = (wed.StartDate - datetime.timedelta(days=wed.StartDate.weekday()))
            # print("Monday2:", monday2, (monday-monday2).days)
            diff = (monday - monday2).days // 7
            # print(monday2, diff)
            if diff < 52:
                weeklyScheduledExecutions[51-diff] += 1
        if wed.EndDate:
            # print('finishedDate:'+str(finishedDate))
            # get difference in weeks
            monday2 = (wed.EndDate - datetime.timedelta(days=wed.EndDate.weekday()))
            diff = (monday - monday2).days // 7
            # print("Monday2:",monday2, diff)
            if diff < 52:
                weeklyFinishedExecutions[51-diff] += 1
    return {'ScheduledExecutions': weeklyScheduledExecutions, 'ProcessedExecutions': weeklyFinishedExecutions}


def getMonthlyExecutionStat():
    return


def getGlobalStat():
    return restApiControl.getExecutionStatistics()


def usage():
    print("schedulerstat [-w] [-h]")
    print("  -w generates weekly (last 52 weeks) statistics")
    print("  -h generates hourly statistics")


if __name__ == '__main__':
    exit(0)  # temporarily disable

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

 
