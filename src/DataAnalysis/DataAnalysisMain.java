package DataAnalysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Locale;
import java.util.StringTokenizer;
import java.util.Vector;

import helloWorld.AtmosphericCompensation;
import helloWorld.UsageData;
import helloWorld.UsagePoint;
import helloWorld.WeatherData;
//import helloWorld.Data;
import helloWorld.WellData;

public class DataAnalysisMain
{
	String currentWorkingDirectory;
	String rootDir;
	File configFile;
	ArrayList<WellPointNew> wellData;
	ArrayList<UsagePoint> usageData;
	ArrayList<Rise> riseTimes;
	ArrayList<RainPoint> rainPoints;
	ArrayList<Peak> peaks;
	DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
	DateTimeFormatter dFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
	String rainSeasonStart = "09-01";
	boolean SUBSET = false;
	boolean PREPROCESS = true;
	LocalDateTime subsetStart = LocalDateTime.of(2023, 10, 21, 0, 0);
	LocalDateTime subsetEnd = LocalDateTime.of(2023, 12, 24, 23, 59);
	// minimum number of down readings to qualify as a peak
	static int DOWNPOINTS = 9;
	// minimum minutes between peaks. If less than this it's not a peak
	static int PEAKMINSEPARATION = 120;

	public static void main(String[] args)
	{
		new DataAnalysisMain();
	}

	public DataAnalysisMain()
	{
		setup();
		processRainfall();
		processUsage();
		processRiseTimes();
		processPeaks();
		out("Data analysis processing completed");
	}

	private void setup()
	{
		// wellData = new ArrayList<WellPointNew>();
		currentWorkingDirectory = System.getProperty("user.dir");
		out("Working Directory = " + currentWorkingDirectory);
		configFile = getConfigFile(currentWorkingDirectory);
		out("Configuration file = " + configFile.getAbsolutePath());
		// Run preProcess only if the raw data has changed. It takes a while
		if (PREPROCESS)
			preProcess(rootDir);
		if (SUBSET)
		{
			out("Well data subset from " + subsetStart + " to " + subsetEnd);
			readWellData(rootDir + "/output/wellReadings.data", subsetStart, subsetEnd);
		}
		else
		{
			out("Full well data");
			readWellData(rootDir + "/Output/WellReadings.data", null, null);
		}
		readUsageData(rootDir + "/output/usage.data");
	}

	private void processRainfall()
	{
		rainPoints = new ArrayList<RainPoint>();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		try
		{
			String fn = rootDir + "/Output/DailyPrecipitation.data";
			out("Reading:" + fn);
			BufferedReader br = new BufferedReader(new FileReader(fn));
			while (br.ready())
			{
				RainPoint wp = new RainPoint();
				String line = br.readLine();
				String[] tokens = line.split("\t");
				if (tokens.length == 1)
					tokens = line.split(",");
				wp.date = LocalDate.parse(tokens[0], formatter);
				wp.rainfall = Double.parseDouble(tokens[1]);
				rainPoints.add(wp);
			}
			writeWeeklyMonthlyYearlyRainfall(rootDir);
			br.close();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void processUsage()
	{
		writeWeeklyMonthlyYearlyUsage(rootDir + "/output/");
	}

	private void processRiseTimes()
	{
		riseTimes = calculateRiseTimes();
		writeRiseTimes(rootDir + "/output/RiseTimes.data");
		writeWeeklyMonthlyYearlyRiseTimes(rootDir);
	}

	private void processPeaks()
	{
		peaks = findPeaks(wellData);
		writeAllPeaks(peaks, rootDir);
		WritePeakStatistics();
	}
	// Establish working directories, read in usage and well data

	public void WritePeakStatistics()
	{
		double avg = getAveragePeakByYear(2021, peaks);
		out("Average peak 2021=" + fmt(avg));
		avg = getAveragePeakByYear(2022, peaks);
		out("Average peak 2022=" + fmt(avg));
		avg = getAveragePeakByYear(2023, peaks);
		out("Average peak 2023=" + fmt(avg));
		writeWeeklyMonthlyYearlyPeakAverages(rootDir, peaks);
	}

	private void readWellData(String fileName, LocalDateTime start, LocalDateTime end)
	{
		out("Reading:" + fileName);
		wellData = new ArrayList<WellPointNew>();
		try
		{
			final String delims = "\t";
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			String line;
			br.readLine(); // skip header
			int count = 0;
			while (br.ready())
			{
				WellPointNew wp = new WellPointNew();
				line = br.readLine();
				String[] tokens = line.split(delims);
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
				wp.dateTime = LocalDateTime.parse(tokens[0], formatter);
				if (start != null)
				{
					if (wp.dateTime.isBefore(start) || wp.dateTime.isAfter(end))
						continue;
				}
				wp.compensatedDepth = Double.parseDouble(tokens[1]);
				wp.originalDepth = Double.parseDouble(tokens[2]);
				wp.waterTemp = Double.parseDouble(tokens[3]);
				if (isNumeric(tokens[4]))
					wp.airTemp = Double.parseDouble(tokens[4]);
				else
					wp.airTemp = Double.NaN;
				wellData.add(wp);
				count++;
			}
			out(count + " well points read");
			br.close();
		}
		catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void readUsageData(String file)
	{
		final String delims = "[\t,]";
		DateTimeFormatter dtf2 = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		int linesRead = 0;
		usageData = new ArrayList<UsagePoint>();
		try
		{
			BufferedReader br = new BufferedReader(new FileReader(file));
			out("Reading:" + file);
			while (br.ready())
			{
				UsagePoint up = new UsagePoint();
				String line = br.readLine();
				String[] tokens = line.split(delims);
				up.date = LocalDate.parse(tokens[0], dtf2);
				up.gallons = Double.parseDouble(tokens[1]);
				usageData.add(up);
				linesRead++;
			}
			out(linesRead + " usage lines read");
			br.close();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// Do this only if the raw data changes
	private void preProcess(String rootDir)
	{
		String fn1 = rootDir + "/usage";
		out("Reading directory:"+fn1);
		UsageData usageData = new UsageData(fn1);
		String fn2 = rootDir + "/output/usage.data";
		out("Writing:"+fn2);
		usageData.write(fn2);
		WeatherData weatherData = new WeatherData(rootDir);
		out("Weather data date range:"+weatherData.getHourlyDateRange());
		weatherData.writeDaily(rootDir);
		WellData wellData = new WellData(rootDir + "/WellData", null);
		String fn3 = rootDir + "/WeatherData/WeatherDataErrors.txt";
		BufferedWriter bw;
		try
		{
			bw = new BufferedWriter(new FileWriter(fn3));
			AtmosphericCompensation.calculate(wellData, weatherData, bw);
			bw.close();
		}
		catch (IOException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		wellData.write(rootDir + "/Output/WellReadings.data");
		out("Preprocessing of raw data completed");
		out("Weather data errors are in:"+fn3);
	}

	private void writeWeeklyMonthlyYearlyRiseTimes(String rootDir)
	{
		int[] yearsRepresented = getUsageYearsRepresented();
		ArrayList<WeekUsage> weeks = new ArrayList<WeekUsage>();
		try
		{
			String fn1 = rootDir + "/output/" + "YearlyRiseTimes.data";
			String fn2 = rootDir + "/output/" + "MonthlyRiseTimes.data";
			String fn3 = rootDir + "/output/" + "WeeklyRiseTimes.data";
			BufferedWriter bw1 = new BufferedWriter(new FileWriter(fn1));
			BufferedWriter bw2 = new BufferedWriter(new FileWriter(fn2));
			BufferedWriter bw3 = new BufferedWriter(new FileWriter(fn3));
			out("Writing:" + fn1);
			out("Writing:" + fn2);
			out("Writing:" + fn3);
			for (int x : yearsRepresented)
			{
				// int year = it.next();
				int year = x;
				// [0] = total, [1] = average
				double averageAnnual = getYearAverageDailyRiseTime(year);
				bw1.write(year + "\t" + fmt(averageAnnual) + "\n");
				for (int month = 1; month < 13; month++)
				{
					double average = getMonthlyAverageDailyRiseTimes(month, year);
					LocalDate date = LocalDate.of(year, month, 28);
					bw2.write(date + "\t" + fmt(average) + "\n");
				}
				for (int week = 1; week < 54; week++)
				{
					Rise rise = getWeekAverageDailyRiseTimes(week, year);
					if (rise.date == null)
						continue;
					String dt = rise.date.format(dFormatter);
					bw3.write(dt + "\t" + fmt(rise.minutes) + "\n");
				}
			}
			bw1.close();
			bw2.close();
			bw3.close();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void writeWeeklyMonthlyYearlyRainfall(String rootDir)
	{
		int[] yearsRepresented = getRainfallYearsRepresented();
		// ArrayList<WeekUsage> weeks = new ArrayList<WeekUsage>();
		try
		{
			String fn1 = rootDir + "/output/" + "YearlyRainfall.data";
			String fn2 = rootDir + "/output/" + "MonthlyRainfall.data";
			String fn3 = rootDir + "/output/" + "WeeklyRainfall.data";
			BufferedWriter bw1 = new BufferedWriter(new FileWriter(fn1));
			BufferedWriter bw2 = new BufferedWriter(new FileWriter(fn2));
			BufferedWriter bw3 = new BufferedWriter(new FileWriter(fn3));
			out("Writing:" + fn1);
			out("Writing:" + fn2);
			out("Writing:" + fn3);
			for (int x : yearsRepresented)
			{
				int year = x;
				double averageAnnual = getYearAverageDailyRainfall(year);
				bw1.write(year + "\t" + fmt(averageAnnual) + "\n");
				for (int month = 1; month < 13; month++)
				{
					double average = getMonthlyTotalRainfall(month, year);
					LocalDate date = LocalDate.of(year, month, 28);
					bw2.write(date + "\t" + fmt(average) + "\n");
				}
				for (int week = 1; week < 54; week++)
				{
					RainPoint wp = getWeekTotalRainfall(week, year);
					if (wp.date == null)
						continue;
					String dt = wp.date.format(dFormatter);
					bw3.write(dt + "\t" + fmt(wp.rainfall) + "\n");
				}
			}
			bw1.close();
			bw2.close();
			bw3.close();
			// Process Rain Year
			// A rain year runs from September through August of the next year.
			// We output only month and day so they can be plotted against each other in one
			// chart
			for (int i = 0; i < yearsRepresented.length - 1; i++)
			{
				String name1 = rootDir + "/output/rainyear " + yearsRepresented[i] + "-" + yearsRepresented[i + 1]
						+ ".data";
				out("Writing:" + name1);
				BufferedWriter bw4 = new BufferedWriter(new FileWriter(name1));
				Iterator<RainPoint> it = rainPoints.iterator();
				double cumulativeRain = 0d;
				while (it.hasNext())
				{
					RainPoint rp = it.next();
					int month = rp.date.getMonthValue();
					int year = rp.date.getYear();
					int day = rp.date.getDayOfMonth();
					String out = "";
					if (year == yearsRepresented[i] && month >= 9)
					{
						cumulativeRain += rp.rainfall;
						out = "00-" + String.format("%02d", month) + "-" + String.format("%02d", day) + "\t"
								+ String.format("%02.2f", rp.rainfall) + "\t" + String.format("%02.2f", cumulativeRain);
						// out("kept:" + out);
						bw4.write(out + "\n");
					}
					else if (year == yearsRepresented[i + 1] && (month < 9))
					{
						cumulativeRain += rp.rainfall;
						out = "01-" + String.format("%02d", month) + "-" + String.format("%02d", day) + "\t"
								+ String.format("%02.2f", rp.rainfall) + "\t" + String.format("%02.2f", cumulativeRain);
						// out("kept:" + out);
						bw4.write(out + "\n");
					}
					// else
					// out("skipped:" + rp.date + " " + rp.rainfall);
				}
				bw4.close();
			}
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void writeWeeklyMonthlyYearlyUsage(String Dir)
	{
		int[] yearsRepresented = getUsageYearsRepresented();
		ArrayList<WeekUsage> weeks = new ArrayList<WeekUsage>();
		try
		{
			String fn1 = Dir + "YearlyUsage.data";
			String fn2 = Dir + "MonthlyUsage.data";
			String fn3 = Dir + "WeeklyUsage.data";
			BufferedWriter bw1 = new BufferedWriter(new FileWriter(fn1));
			BufferedWriter bw2 = new BufferedWriter(new FileWriter(fn2));
			BufferedWriter bw3 = new BufferedWriter(new FileWriter(fn3));
			out("Writing:" + fn1);
			out("Writing:" + fn2);
			out("Writing:" + fn3);
			// BufferedWriter bw4 = new BufferedWriter(new FileWriter(rootDir +
			// "DailyUsage.data"));
			for (int x : yearsRepresented)
			{
				// int year = it.next();
				int year = x;
				// [0] = total, [1] = average
				double[] usage = getYearTotalAndAverageDailyUsage(year);
				bw1.write(year + "\t" + usage[0] + "\t" + fmt(usage[1]) + "\n");
				for (int month = 1; month < 13; month++)
				{
					double[] mUsage = getMonthlyTotalAndAverageDailyUsage(month, year);
					LocalDate date = LocalDate.of(year, month, 28);
					// bw2.write(year+"-"+month + "\t" + mUsage + "\n");
					bw2.write(date + "\t" + mUsage[0] + "\t" + fmt(mUsage[1]) + "\n");
				}
				for (int week = 1; week < 54; week++)
				{
					WeekUsage wu = getWeekTotalAndAverageDailyUsage(week, year);
					if (wu.date != null)
						weeks.add(wu);
				}
			}
			writeWeeksUsage(weeks, bw3);
			bw1.close();
			bw2.close();
			bw3.close();
			// out(Dir + "YearlyUsage.data written");
			// out(Dir + "MonthlyUsage.data written");
			// out(Dir + "WeeklyUsage.data written");
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void writeWeeklyMonthlyYearlyPeakAverages(String rootDir, ArrayList<Peak> peaks)
	{
		int[] yearsRepresented = getUsageYearsRepresented();
		try
		{
			String fn1 = rootDir + "/output/YearlyPeaks.data";
			String fn2 = rootDir + "/output/MonthlyPeaks.data";
			String fn3 = rootDir + "/output/WeeklyPeaks.data";
			BufferedWriter bw1 = new BufferedWriter(new FileWriter(fn1));
			BufferedWriter bw2 = new BufferedWriter(new FileWriter(fn2));
			BufferedWriter bw3 = new BufferedWriter(new FileWriter(fn3));
			out("Writing:" + fn1);
			out("Writing:" + fn2);
			out("Writing:" + fn3);
			for (int i = 0; i < yearsRepresented.length; i++)
			{
				int year = yearsRepresented[i];
				double peaksAverage = getYearPeaksAverage(year);
				bw1.write(year + "\t" + fmt(peaksAverage) + "\n");
				Peak[] averageMonthlyPeaks = getMonthPeakAverage(year);
				for (int j = 0; j < averageMonthlyPeaks.length; j++)
				{
					if (averageMonthlyPeaks[j].dateTime == null)
						continue;
					String dt = averageMonthlyPeaks[j].dateTime.format(dFormatter);
					bw2.write(dt + "\t" + fmt(averageMonthlyPeaks[j].value) + "\n");
				}
				Peak[] averageWeeklyPeak = getWeekPeakAverage(year);
				for (int k = 0; k < averageWeeklyPeak.length; k++)
				{
					if (averageWeeklyPeak[k].dateTime == null)
						continue;
					String dt = averageWeeklyPeak[k].dateTime.format(dFormatter);
					bw3.write(dt + "\t" + fmt(averageWeeklyPeak[k].value) + "\n");
				}
			}
			// writeWeeksUsage(weeks, bw3);
			bw1.close();
			bw2.close();
			bw3.close();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void writeWeeksUsage(ArrayList<WeekUsage> weeks, BufferedWriter bw)
	{
		Collections.sort(weeks, new Comparator<WeekUsage>()
		{
			public int compare(WeekUsage o1, WeekUsage o2)
			{
				return o1.date.compareTo(o2.date);
			}
		});
		Iterator<WeekUsage> it = weeks.iterator();
		while (it.hasNext())
		{
			WeekUsage wu = it.next();
			try
			{
				bw.write(wu.date + "\t" + fmt(wu.usage) + "\t" + fmt(wu.avg) + "\n");
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static boolean isNumeric(String str)
	{
		try
		{
			Double.parseDouble(str);
			return true;
		}
		catch (NumberFormatException e)
		{
			return false;
		}
	}

	private void out(String str)
	{
		System.out.println(str);
	}

	private File getConfigFile(String directory)
	{
		configFile = new File(directory + "/DataAnalysis.cfg");
		if (!configFile.exists())
		{
			writeConfigFile();
		}
		else
		{
			try
			{
				BufferedReader br = new BufferedReader(new FileReader(configFile));
				rootDir = (br.readLine());
				br.close();
				out("Rootdir=" + rootDir);
			}
			catch (FileNotFoundException e)
			{
				out(configFile + " file not found");
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		return configFile;
	}

	private void writeConfigFile()
	{
		try
		{
			BufferedWriter bw = new BufferedWriter(new FileWriter(configFile));
			bw.write("d:\\Kir-Nel\n");
			rootDir = "d:\\Kir-Nel";
			bw.close();
			System.out.println("Created " + configFile);
		}
		catch (IOException e1)
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public Peak[] getMonthPeakAverage(int year)
	{
		Iterator<Peak> it = peaks.iterator();
		Peak[] averagePeaks = new Peak[12];
		int[] count = new int[12];
		for (int i = 0; i < 12; i++)
		{
			averagePeaks[i] = new Peak();
			count[i] = 1;
		}
		while (it.hasNext())
		{
			Peak peak = it.next();
			if (peak.dateTime.getYear() == year)
			{
				int month = peak.dateTime.getMonth().getValue();
				averagePeaks[month - 1].value += (peak.value - averagePeaks[month - 1].value) / count[month - 1];
				averagePeaks[month - 1].dateTime = LocalDateTime.of(year, month, 15, 0, 0);
				++count[month - 1];
			}
		}
		return averagePeaks;
	}

	public double getMonthlyTotalRainfall(int month, int year)
	{
		Iterator<RainPoint> it = rainPoints.iterator();
		double out = 0d;
		// int t = 1;
		// double total = 0d;
		while (it.hasNext())
		{
			RainPoint wp = it.next();
			if (wp.date.getYear() == year)
			{
				int mon = wp.date.getMonth().getValue();
				if (wp.date.getMonthValue() == month)
				{
					out += wp.rainfall;
					// out += (wp.rainfall - out) / t;
					// ++t;
				}
			}
		}
		return out;
	}

	// Returns average rise times for the month
	public double getMonthlyAverageDailyRiseTimes(int month, int year)
	{
		Iterator<Rise> it = riseTimes.iterator();
		double out = 0d;
		int t = 1;
		// double total = 0d;
		while (it.hasNext())
		{
			Rise rise = it.next();
			if (rise.date.getYear() == year)
			{
				int mon = rise.date.getMonth().getValue();
				if (rise.date.getMonthValue() == month)
				{
					out += (rise.minutes - out) / t;
					++t;
				}
			}
		}
		return out;
	}

	public double[] getMonthlyTotalAndAverageDailyUsage(int month, int year)
	{
		Iterator<UsagePoint> it = usageData.iterator();
		double[] out = new double[2];
		out[0] = 0;
		out[1] = 0;
		int t = 1;
		while (it.hasNext())
		{
			UsagePoint usage = it.next();
			if (usage.date.getYear() == year)
			{
				int mon = usage.date.getMonth().getValue();
				if (usage.date.getMonthValue() == month)
				{
					out[0] += usage.gallons;
					out[1] += (usage.gallons - out[1]) / t;
					++t;
				}
			}
		}
		return out;
	}

	public double getYearAverageDailyRiseTime(int year)
	{
		Iterator<Rise> it = riseTimes.iterator();
		double out = 0d;
		int t = 1;
		while (it.hasNext())
		{
			Rise rise = it.next();
			if (rise.date.getYear() == year)
			{
				out += (rise.minutes - out) / t;
				++t;
			}
		}
		return out;
	}

	public double getYearAverageDailyRainfall(int year)
	{
		Iterator<RainPoint> it = rainPoints.iterator();
		double out = 0d;
		int t = 1;
		while (it.hasNext())
		{
			RainPoint wp = it.next();
			if (wp.date.getYear() == year)
			{
				out += (wp.rainfall - out) / t;
				++t;
			}
		}
		return out;
	}

	// [0] = total for the year, [1] = average daily usage for the year
	public double[] getYearTotalAndAverageDailyUsage(int year)
	{
		Iterator<UsagePoint> it = usageData.iterator();
		double[] out = new double[2];
		out[0] = 0d;
		out[1] = 0d;
		int t = 1;
		while (it.hasNext())
		{
			UsagePoint up = it.next();
			if (up.date.getYear() == year)
			{
				out[0] += up.gallons.doubleValue();
				out[1] += (up.gallons.doubleValue() - out[1]) / t;
				++t;
			}
		}
		return out;
	}

	public double getYearPeaksAverage(int year)
	{
		Iterator<Peak> it = peaks.iterator();
		double out = 0d;
		int t = 1;
		while (it.hasNext())
		{
			Peak peak = it.next();
			if (peak.dateTime.getYear() == year)
			{
				out += (peak.value - out) / t;
				++t;
			}
		}
		return out;
	}

	// Returns week's average rise time plus a Monday for that week
	public Rise getWeekAverageDailyRiseTimes(int weekIn, int yearIn)
	{
		Rise riseOut = new Rise();
		riseOut.minutes = 0d;
		int t = 1;
		Iterator<Rise> it = riseTimes.iterator();
		// double total = 0d;
		// TemporalField tf1 = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();
		TemporalField tf2 = WeekFields.of(Locale.getDefault()).weekOfYear();
		while (it.hasNext())
		{
			Rise rise = it.next();
			// riseOut.date = rise.date;
			int weekOfYear = rise.date.get(tf2);
			// int week = rise.date.get(tf1);
			// int month = rise.date.getMonthValue();
			if (weekIn == weekOfYear && rise.date.getYear() == yearIn)
			{
				riseOut.date = rise.date;
				riseOut.minutes += (rise.minutes - riseOut.minutes) / t;
				++t;
			}
		}
		if (riseOut.minutes > 0)
		{
			LocalDateTime monday;
			monday = riseOut.date.with(TemporalAdjusters.next(DayOfWeek.MONDAY));
			riseOut.date = monday;
		}
		else
			riseOut.date = null;
		return riseOut;
	}

	public RainPoint getWeekTotalRainfall(int weekIn, int yearIn)
	{
		RainPoint wpOut = new RainPoint();
		wpOut.rainfall = 0d;
		int t = 1;
		Iterator<RainPoint> it = rainPoints.iterator();
		// double total = 0d;
		// TemporalField tf1 = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();
		TemporalField tf2 = WeekFields.of(Locale.getDefault()).weekOfYear();
		while (it.hasNext())
		{
			RainPoint wp = it.next();
			// riseOut.date = rise.date;
			int weekOfYear = wp.date.get(tf2);
			if (weekIn == weekOfYear && wp.date.getYear() == yearIn)
			{
				wpOut.date = wp.date;
				wpOut.rainfall += wp.rainfall;
				// wpOut.rainfall += (wp.rainfall - wpOut.rainfall) / t;
				// ++t;
			}
		}
		if (wpOut.rainfall > 0)
		{
			LocalDate friday;
			friday = wpOut.date.with(TemporalAdjusters.next(DayOfWeek.FRIDAY));
			wpOut.date = friday;
		}
		else
			wpOut.date = null;
		return wpOut;
	}

	public WeekUsage getWeekTotalAndAverageDailyUsage(int week, int year)
	{
		WeekUsage wu = new WeekUsage();
		wu.usage = 0l;
		int t = 1;
		Iterator<UsagePoint> it = usageData.iterator();
		// double total = 0d;
		TemporalField tf = WeekFields.of(Locale.getDefault()).weekOfYear();
		while (it.hasNext())
		{
			UsagePoint up = it.next();
			int weekOfYear = up.date.get(tf);
			if (week == weekOfYear && up.date.getYear() == year)
			{
				wu.usage += up.gallons;
				wu.date = up.date;
				wu.avg += (up.gallons - wu.avg) / t;
				++t;
			}
		}
		LocalDate monday;
		if (wu.date != null)
		{
			monday = wu.date.with(TemporalAdjusters.next(DayOfWeek.MONDAY));
			wu.date = monday;
		}
		return wu;
	}

	public Peak[] getWeekPeakAverage(int year)
	{
		Peak[] weekAverages = new Peak[54];
		int[] counts = new int[54];
		for (int i = 0; i < counts.length; i++)
		{
			counts[i] = 1;
			weekAverages[i] = new Peak();
		}
		TemporalField tf = WeekFields.of(Locale.getDefault()).weekOfYear();
		Iterator<Peak> it = peaks.iterator();
		while (it.hasNext())
		{
			Peak peak = it.next();
			if (peak.dateTime.getYear() != year)
				continue;
			int weekOfYear = peak.dateTime.get(tf);
			// if (weekOfYear > 50)
			// out("stop");
			weekAverages[weekOfYear].value += (peak.value - weekAverages[weekOfYear].value) / counts[weekOfYear];
			++counts[weekOfYear];
			if (weekAverages[weekOfYear].dateTime == null)
			{
				weekAverages[weekOfYear].dateTime = peak.dateTime.with((TemporalAdjusters.next(DayOfWeek.MONDAY)));
			}
		}
		return weekAverages;
	}

	private void temporalFieldExperiment()
	{
		TemporalField tfx;
		LocalDate d1 = LocalDate.of(2022, 1, 1);
		tfx = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();
		int w1 = d1.get(tfx);
		tfx = WeekFields.of(Locale.getDefault()).weekOfYear();
		int w2 = d1.get(tfx);
		LocalDate d2 = LocalDate.of(2022, 12, 31);
		tfx = WeekFields.of(Locale.getDefault()).weekOfWeekBasedYear();
		int w3 = d2.get(tfx);
		tfx = WeekFields.of(Locale.getDefault()).weekOfYear();
		int w4 = d2.get(tfx);
	}

	public int[] getUsageYearsRepresented()
	{
		Vector<Integer> yrs = new Vector<Integer>();
		Iterator<UsagePoint> it = usageData.iterator();
		while (it.hasNext())
		{
			UsagePoint up = it.next();
			Integer yr = up.date.getYear();
			if (!yrs.contains(yr))
				yrs.add(yr);
		}
		int[] yr = new int[yrs.size()];
		for (int i = 0; i < yrs.size(); i++)
			yr[i] = yrs.get(i);
		return yr;
	}

	public int[] getRainfallYearsRepresented()
	{
		Vector<Integer> yrs = new Vector<Integer>();
		Iterator<RainPoint> it = rainPoints.iterator();
		while (it.hasNext())
		{
			RainPoint up = it.next();
			Integer yr = up.date.getYear();
			if (!yrs.contains(yr))
				yrs.add(yr);
		}
		int[] yr = new int[yrs.size()];
		for (int i = 0; i < yrs.size(); i++)
			yr[i] = yrs.get(i);
		return yr;
	}

	private ArrayList<Rise> calculateRiseTimes()
	{
		WellPointNew baseWellPoint = null;
		WellPointNew previousWellPoint = null;
		riseTimes = new ArrayList<Rise>();
		Iterator<WellPointNew> it = wellData.iterator();
		while (it.hasNext())
		{
			WellPointNew wpn = it.next();
			if (wpn.compensatedDepth < 0d)
				continue;
			if (baseWellPoint == null)
			{
				baseWellPoint = wpn;
				previousWellPoint = wpn;
				continue;
			}
			// Continued rise in depth
			if (wpn.compensatedDepth >= previousWellPoint.compensatedDepth)
			{
				double diff = wpn.compensatedDepth - baseWellPoint.compensatedDepth;
				if (diff >= 400)
				{
					long time = calculateMinutesDifference(baseWellPoint.dateTime, wpn.dateTime);
					Rise rise = new Rise();
					rise.date = baseWellPoint.dateTime;
					rise.minutes = time;
					riseTimes.add(rise);
					// out(fmt(diff) + " cm over " + time + " minutes");
					baseWellPoint = wpn;
				}
				// out ("Rise of "+(fmt(wpn.compensatedDepth -
				// previousWellPoint.compensatedDepth)));
				previousWellPoint = wpn;
				continue;
			}
			// Rise in depth stops
			else
			{
				long time = calculateMinutesDifference(previousWellPoint.dateTime, wpn.dateTime);
				// out("Rise stopped, diff="+(wpn.compensatedDepth -
				// baseWellPoint.compensatedDepth));
				baseWellPoint = wpn;
				previousWellPoint = wpn;
				continue;
			}
		}
		out(riseTimes.size() + " rise events");
		return riseTimes;
	}

	private String fmt(double d)
	{
		return String.format("%.2f", d);
		// return Double.toString(d);
	}

	private long calculateMinutesDifference(LocalDateTime lo, LocalDateTime hi)
	{
		Duration duration = Duration.between(lo, hi);
		long seconds = duration.getSeconds();
		return seconds / 60;
	}

	private void writeRiseTimes(String fileName)
	{
		try
		{
			BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
			out("Writing:" + fileName);
			Iterator<Rise> it = riseTimes.iterator();
			while (it.hasNext())
			{
				Rise rise = it.next();
				String dt = rise.date.format(dtFormatter);
				bw.write(dt + "\t" + rise.minutes + "\n");
			}
			bw.close();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private ArrayList<Peak> findPeaks(ArrayList<WellPointNew> pointsIn)
	{
		ArrayList<Peak> peaksOut = new ArrayList<Peak>();
		ArrayList<WellPointNew> pointsToProcess = null;
		WellPointNew lastPeak = null;
		int upPoints = 0;
		int downPoints = 0;
		if (pointsIn == null)
			pointsToProcess = wellData;
		else
			pointsToProcess = pointsIn;
		Iterator<WellPointNew> it = pointsToProcess.iterator();
		// Go through each wellpoint looking for upslope.
		for (WellPointNew wp : pointsToProcess)
		{
			if (wp.compensatedDepth <= 0d)
				continue;
			int index = pointsToProcess.indexOf(wp) + 1;
			if (index >= pointsToProcess.size())
				break;
			double d1 = wp.compensatedDepth;
			double d2 = pointsToProcess.get(pointsToProcess.indexOf(wp) + 1).compensatedDepth;
			double diff = calcDiff(wp, pointsToProcess);
			if (diff == Double.NaN)
				break;
			if (diff > 0d) // headed up
			{
				if (downPoints > 0)
					downPoints = 0;
				upPoints++;
				continue;
			}
			else // headed down
			{
				if (upPoints > 0)
				{
					lastPeak = wp;
					upPoints = 0;
				}
				downPoints++;
				if (downPoints > DOWNPOINTS && lastPeak != null)
				{
					peaksOut.add(new Peak(lastPeak.dateTime, lastPeak.compensatedDepth));
					downPoints = 0;
					// Duration duration = Duration.between(lastPeak.dateTime, wp.dateTime);
					// long minutes = duration.toMinutes();
					// out (minutes+" minutes separation");
					// if (minutes > PEAKMINSEPARATION)
					// {
					// peaksOut.add(new Peak(lastPeak.dateTime, wp.compensatedDepth));
					// lastPeak = null;
					// downPoints = 0;
					// }
				}
			}
		}
		out("FindPeaks " + peaksOut.size() + " peaks found");
		return peaksOut;
	}

	private double calcDiff(WellPointNew wp, ArrayList<WellPointNew> points)
	{
		double a = wp.compensatedDepth;
		int index = points.indexOf(wp) + 1;
		double b = points.get(index).compensatedDepth;
		double diff = b - a;
		return diff;
	}

	private ArrayList<Peak> xxxfindPeaks(ArrayList<WellPointNew> pointsIn)
	{
		ArrayList<Peak> peaksOut = new ArrayList<Peak>();
		ArrayList<WellPointNew> pointsToProcess = null;
		if (pointsIn == null)
			pointsToProcess = wellData;
		else
			pointsToProcess = pointsIn;
		Iterator<WellPointNew> it = pointsToProcess.iterator();
		WellPointNew lastPeak = null;
		while (it.hasNext())
		{
			WellPointNew wp = it.next();
			LocalDateTime theDate = LocalDateTime.parse("2023-10-21T00:00:00.000");
			if (wp.dateTime.isAfter(theDate))
				out("stoppers");
			if (wp.compensatedDepth < 300)
				continue;
			// out("wellpoint " + wp.dateTime + " " + wp.compensatedDepth);
			int index = pointsToProcess.indexOf(wp);
			// Get next DOWNPOINTS+1 points
			WellPointNew[] points = getNextPoints(index, pointsToProcess);
			if (points == null)
				break;
			// outNextPoints(points);
			int i;
			for (i = 0; i < DOWNPOINTS; i++)
			{
				double diff = points[i + 1].compensatedDepth - points[i].compensatedDepth;
				if (diff >= 0d || (diff <= 0 && diff > -10))
				{
					out("climbing by " + diff);
					// break;
					continue;
				}
				out("dropping by " + diff);
				if (i >= DOWNPOINTS - 1)
				{
					// If this peak is within 2 hours of last peak, throw it out
					if (lastPeak != null)
					{
						Duration duration = Duration.between(lastPeak.dateTime, wp.dateTime);
						long minutes = duration.toMinutes();
						if (minutes < PEAKMINSEPARATION)
						{
							// out("Peak too close in time:" + minutes + " " + lastPeak.dateTime + " " +
							// wp.dateTime);
							continue;
						}
					}
					// out("peak " + wp.compensatedDepth + " " + wp.dateTime);
					// Peak peak = new Peak();
					// peak.dateTime = wp.dateTime;
					// peak.value = wp.compensatedDepth;
					// peaksOut.add(peak);
					lastPeak = wp;
					break;
				}
				continue;
			}
		}
		out(peaksOut.size() + " peaks");
		return peaksOut;
	}

	private void outNextPoints(WellPointNew[] points)
	{
		System.out.print("Next points:");
		for (int i = 0; i < points.length; i++)
			System.out.print(points[i].compensatedDepth + ", ");
		System.out.println("");
	}

	private void writeAllPeaks(ArrayList<Peak> peaksIn, String rootDir)
	{
		try
		{
			String fn1 = rootDir + "/output/peaks.data";
			BufferedWriter bw = new BufferedWriter(new FileWriter(fn1));
			out("Writing:" + fn1);
			Iterator<Peak> it = peaksIn.iterator();
			while (it.hasNext())
			{
				Peak peak = it.next();
				String dt = peak.dateTime.format(dtFormatter);
				bw.write(dt + "\t" + peak.value + "\n");
			}
			bw.close();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	//
	// Get the next set of (DOWNPOINTS + 1) well points
	//
	private WellPointNew[] getNextPoints(int index, ArrayList<WellPointNew> pointsToProcess)
	{
		WellPointNew[] wpma = new WellPointNew[DOWNPOINTS + 1];
		for (int i = index, j = 0; i < index + DOWNPOINTS + 1; i++, j++)
		{
			if (i >= pointsToProcess.size())
				return null;
			wpma[j] = pointsToProcess.get(i);
		}
		return wpma;
	}

	private ArrayList<WellPointNew> subsetWellReadings(LocalDateTime start, LocalDateTime end)
	{
		ArrayList<WellPointNew> out = new ArrayList<WellPointNew>();
		Iterator<WellPointNew> it = wellData.iterator();
		while (it.hasNext())
		{
			WellPointNew wp = it.next();
			if (wp.dateTime.isBefore(start))
				continue;
			if (wp.dateTime.isAfter(end))
				continue;
			out.add(wp);
		}
		return out;
	}

	private double getAveragePeakByYear(int year, ArrayList<Peak> peaks)
	{
		ArrayList<Peak> subset = new ArrayList<Peak>();
		Iterator<Peak> it = peaks.iterator();
		double avg = 0;
		int t = 1;
		while (it.hasNext())
		{
			Peak peak = it.next();
			if (peak.dateTime.getYear() == year)
			{
				avg += (peak.value - avg) / t;
				++t;
			}
		}
		return avg;
	}

	double mean(double[] ary)
	{
		double avg = 0;
		int t = 1;
		for (double x : ary)
		{
			avg += (x - avg) / t;
			++t;
		}
		return avg;
	}

	private double getAveragePeakByMonth()
	{
		return 0d;
	}

	private double getAveragePeakByWeek()
	{
		return 0d;
	}

	private class WeekUsage
	{
		LocalDate date;
		long usage;
		double avg;
	}

	private class Rise
	{
		LocalDateTime date;
		double minutes;
	}

	private class Peak
	{
		public Peak()
		{
		}

		public Peak(LocalDateTime dt, double v)
		{
			dateTime = dt;
			value = v;
		}

		LocalDateTime dateTime;
		double value;
	}

	private class RainPoint
	{
		LocalDate date;
		double rainfall;
	}
}
