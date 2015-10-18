#!/usr/bin/env python
import argparse
import logging
import sys
import Gnuplot

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)

class Visualizer(object):

    TIME_UNITS = {"sec": 1, "min": 60, "hour": 3600, "day": 24*3600}
    MEMORY_UNITS = {"B": 1, "KiB": 1024, "MiB": 1024*1024,
                    "GiB": 1024*1024*1024}

    def __init__(self, args):
        self.__args = args

    def __call__(self):

        virt_data = []
        res_data = []
        first_time = None
        time_unit_val = self.TIME_UNITS[self.__args.time_unit]

        input_memory_unit = 1024.0 * 1024.0
        mem_coef = input_memory_unit / self.MEMORY_UNITS[self.__args.memory_unit]
        for line in self.__args.log_file:
            if line[0] == '-':
                continue
            time, virt, res, shm, dt = line.split()

            if first_time is None:
                first_time = float(time)
            elapsed = float(time) - first_time
            elapsed /= time_unit_val

            virt_data.append((elapsed, mem_coef * float(virt)))
            res_data.append((elapsed, mem_coef * float(res)))

        gp = Gnuplot.Gnuplot()
        gp("set terminal png")
        gp("set output \"%s\"" % self.__args.output_file)
        print "len: %d" % len(virt_data)

        gp.xlabel("time (%s)" % self.__args.time_unit);
        gp.ylabel("Used memory (%s)" % self.__args.memory_unit);
        virt_trend_data = Gnuplot.PlotItems.Data(virt_data, with_="lines",
                                                 title="Virt")
        res_trend_data = Gnuplot.PlotItems.Data(res_data, with_="lines",
                                                title="Res")
        gp.plot(virt_trend_data, res_trend_data)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("log_file", type=file)
    parser.add_argument("output_file", type=str)
    parser.add_argument("-m", "--memory-unit", default="MiB",
                        choices=Visualizer.MEMORY_UNITS)
    parser.add_argument("-t", "--time-unit", default="sec",
                        choices=Visualizer.TIME_UNITS)
    args = parser.parse_args()

    visualizer = Visualizer(args)
    visualizer()


if __name__ == "__main__":
    main()
