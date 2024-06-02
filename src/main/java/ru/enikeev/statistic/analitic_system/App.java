package ru.enikeev.statistic.analitic_system;

import java.io.FileWriter;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;


public class App {
	
	public static String URL = "jdbc:postgresql://localhost:5433/dean_office";
	public static String USER_NAME = "postgres";
	public static String PASSWORD = "s20g;_2-r505t8";
	
	
	public static void main(String[] args) {
		
		StatusBar(4, 2);
		TestAllStudentsResultsTable(5);
		AttendanceByTime(1, 1, "Lecture", 1);
		AttendanceByDiscipline(1, 1);
		TestTimeResultsTable(5,1);
		AcademicPerformanceDisciplineStatistic(2, 1, 1);
		TestPerformanceStatistics(1, 1, 1);
		AccumulatedRating(5);
    }
	
	
	//Курсы 2 (Студент): СТАТУСБАР
	public static void StatusBar(int student_id, int discipline_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs;
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query_all_tests = String.format("select count(test_id) as all_test_count from public.test where discipline_id = %s", discipline_id);
			String query_students_tests = String.format("select count(test_id) as student_test_count from"
					+ " (select count(test_id) as test_id from attempt where student_id = %d and test_id in"
					+ " (select test_id from test where discipline_id = %d)"
					+ "	group by test_id) as a", student_id, discipline_id);
			
			int all_tests = 0, students_tests = 0;
			
			rs = statement.executeQuery(query_all_tests);
			if (rs.next()) all_tests = rs.getInt(1);
			
			rs = statement.executeQuery(query_students_tests);
			if (rs.next()) students_tests = rs.getInt(1);
			
			saveData(students_tests * 1.0 / all_tests, "src\\main\\java\\resources\\output.json");
			
			rs.close();
			statement.close();
			connection.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	//Курсы 5 (Препод): Таблица [ФИО, Группа, Количество правильных ответов, Оценка]
	public static void TestAllStudentsResultsTable(int test_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs, rs_questions;
		
		ArrayList<ArrayList<String>> ans = new ArrayList<>();
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select student_fullname, study_group_number, max_result from study_group join("
					+ "	select student_fullname, study_group_id, max(attempt_result) as max_result from"
					+ "	student join attempt using(student_id)"
					+ "	group by test_id, student_id"
					+ "	having test_id = %d"
					+ "	order by study_group_id, student_fullname"
					+ "	) as a using(study_group_id)", test_id);
			
			String query_count_questions = String.format("select count(question_id) from"
					+ " test join question using(test_id)"
					+ "	where test_id = %d", test_id);
			
			rs_questions = statement.executeQuery(query_count_questions);
			
			int count_questions = 0;
			if (rs_questions.next()) 
				count_questions = rs_questions.getInt("count");
			rs_questions.close();
			
			rs = statement.executeQuery(query);
		
			while (rs.next()) {
				String student_fullname = rs.getString("student_fullname");
				String study_group_number = rs.getString("study_group_number");
				String max_result = String.valueOf(rs.getInt("max_result") / 10);
				String write_answers = String.format("%s/%d", max_result, count_questions);
				ArrayList<String> pod_ans = new ArrayList<>();
				pod_ans.add(student_fullname);
				pod_ans.add(study_group_number);
				pod_ans.add(max_result);
				pod_ans.add(write_answers);
				ans.add(pod_ans);
				
			}
			
			rs.close();
			statement.close();
			connection.close();
			
			saveData(ans, "src\\main\\java\\resources\\output.json");

			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	//Статистика 1 (Препод): Таблица [Количество пришедших, Время прибытия]
	public static void AttendanceByTime(int discipline_id, int study_group_id, String lesson_type, int schedule_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs, rs_lesson_time;
		
		ArrayList<ArrayList<String>> ans = new ArrayList<>();

		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select count(attendance_datetime), attendance_datetime from attendance join schedule using(discipline_id)"
					+ " where discipline_id = %d and study_group_id = %d and attendance_datetime is not null"
					+ " and (lesson_start_datetime = attendance_datetime or lesson_end_datetime = attendance_datetime or"
					+ "	(lesson_start_datetime < attendance_datetime and lesson_end_datetime > attendance_datetime)) and lesson_start_datetime ="
					+ "	(select lesson_start_datetime from schedule where lesson_type = '%s' and discipline_id = 1 and study_group_id = 1 and schedule_id = 1)"
					+ " group by attendance_datetime", discipline_id, study_group_id, lesson_type, discipline_id, study_group_id, schedule_id);
			
			String query_times = String.format("select lesson_start_datetime, lesson_end_datetime from schedule"
					+ " where lesson_type = '%s' and discipline_id = %d and study_group_id = %d and schedule_id = %d", 
					lesson_type, discipline_id, study_group_id, schedule_id);
			
			rs_lesson_time = statement.executeQuery(query_times);
			if (rs_lesson_time.next()) {
				ArrayList<String> pod_ans = new ArrayList<>();
				pod_ans.add(String.valueOf(rs_lesson_time.getTime("lesson_start_datetime")));
				pod_ans.add(String.valueOf(rs_lesson_time.getTime("lesson_end_datetime")));
				ans.add(pod_ans);
			}
			rs_lesson_time.close();
			
			rs = statement.executeQuery(query);
			while (rs.next()) {
				ArrayList<String> pod_ans = new ArrayList<>();
				pod_ans.add(String.valueOf(rs.getInt("count")));
				pod_ans.add(String.valueOf(rs.getTime("attendance_datetime")));
				ans.add(pod_ans);
			}
			rs.close();
			
			statement.close();
			connection.close();
			
			saveData(ans, "src\\main\\java\\resources\\output.json");
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	//Статистика & (Препод-Студент): Таблица [Предмет, Посещения]
	public static void CountAttendanceByDisciplines(int discipline_id, int study_group_id, String lesson_type, int schedule_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs, rs_lesson_time;
		
		ArrayList<ArrayList<String>> ans = new ArrayList<>();

		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select count(attendance_datetime), attendance_datetime from attendance join schedule using(discipline_id)"
					+ " where discipline_id = %d and study_group_id = %d and attendance_datetime is not null"
					+ " and (lesson_start_datetime = attendance_datetime or lesson_end_datetime = attendance_datetime or"
					+ "	(lesson_start_datetime < attendance_datetime and lesson_end_datetime > attendance_datetime)) and lesson_start_datetime ="
					+ "	(select lesson_start_datetime from schedule where lesson_type = '%s' and discipline_id = 1 and study_group_id = 1 and schedule_id = 1)"
					+ " group by attendance_datetime", discipline_id, study_group_id, lesson_type, discipline_id, study_group_id, schedule_id);
			
			String query_times = String.format("select lesson_start_datetime, lesson_end_datetime from schedule"
					+ " where lesson_type = '%s' and discipline_id = %d and study_group_id = %d and schedule_id = %d", 
					lesson_type, discipline_id, study_group_id, schedule_id);
			
			rs_lesson_time = statement.executeQuery(query_times);
			if (rs_lesson_time.next()) {
				ArrayList<String> pod_ans = new ArrayList<>();
				pod_ans.add(String.valueOf(rs_lesson_time.getTime("lesson_start_datetime")));
				pod_ans.add(String.valueOf(rs_lesson_time.getTime("lesson_end_datetime")));
				ans.add(pod_ans);
			}
			rs_lesson_time.close();
			
			rs = statement.executeQuery(query);
			while (rs.next()) {
				ArrayList<String> pod_ans = new ArrayList<>();
				pod_ans.add(String.valueOf(rs.getInt("count")));
				pod_ans.add(String.valueOf(rs.getTime("attendance_datetime")));
				ans.add(pod_ans);
			}
			rs.close();
			
			statement.close();
			connection.close();
			
			saveData(ans, "src\\main\\java\\resources\\output.json");
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	//Статистика & (Студент): Таблица [Дата занятия, Посещение true-false]
	public static void AttendanceByDiscipline(int student_id, int discipline_id) {
			
		Connection connection;
		Statement statement;
		ResultSet rs;
		
		ArrayList<ArrayList<String>> ans = new ArrayList<>();

		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select distinct lesson_start_datetime,"
					+ " case when (lesson_start_datetime = attendance_datetime or lesson_end_datetime = attendance_datetime or"
					+ " (lesson_start_datetime < attendance_datetime and lesson_end_datetime > attendance_datetime))"
					+ " then 1 else 0 end as attend"
					+ " from schedule join attendance using(discipline_id) where discipline_id = %d and student_id = %d", discipline_id, student_id);
			
			rs = statement.executeQuery(query);
			while (rs.next()) {
				ArrayList<String> pod_ans = new ArrayList<>();
				pod_ans.add(String.valueOf(rs.getDate("lesson_start_datetime")));
				pod_ans.add(String.valueOf(rs.getInt("attend")));
				ans.add(pod_ans);
			}
			rs.close();
			
			statement.close();
			connection.close();
			
			saveData(ans, "src\\main\\java\\resources\\output.json");
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}
	
	//Статистика 3 (Препод): Таблица [Результаты тестов, Время сдачи тестов]
	public static void TestTimeResultsTable(int test_id, int study_group_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs, rs_test_time;
		
		ArrayList<ArrayList<String>> ans = new ArrayList<>();
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select attempt_datetime, attempt_result from("
					+ " select a.student_id, a.attempt_datetime, a.attempt_result from attempt a"
					+ " where a.test_id = %d and a.attempt_datetime ="
					+ " (select min(a2.attempt_datetime) from attempt a2"
					+ " where a2.student_id = a.student_id and test_id = %d)"
					+ " order by a.attempt_datetime) as b where student_id in("
					+ " select student_id from study_group join student using(study_group_id)"
					+ " where study_group_id = %d)", test_id, test_id, study_group_id);
			
			String query_test_start_end_time = String.format("select test_start, test_end from test where test_id = %d", test_id);
			
			rs_test_time = statement.executeQuery(query_test_start_end_time);
			Timestamp test_start_date_time = null, test_end_date_time = null;
			
			if (rs_test_time.next()) {
				test_start_date_time = rs_test_time.getTimestamp("test_start");
				test_end_date_time = rs_test_time.getTimestamp("test_end");
				
				SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
				
		        ArrayList<String> pod_ans = new ArrayList<>();
		        pod_ans.add(sdf.format(test_start_date_time));
		        pod_ans.add(sdf.format(test_end_date_time));
		        ans.add(pod_ans);
		        
			}
			rs_test_time.close();
			

			rs = statement.executeQuery(query);
			while (rs.next()) {
				
				Time attempt_datetime = rs.getTime("attempt_datetime");
				String attempt_result = String.valueOf(rs.getInt("attempt_result"));
				
				ArrayList<String> pod_ans = new ArrayList<>();
		        pod_ans.add(String.valueOf(attempt_datetime));
		        pod_ans.add(String.valueOf(attempt_result));
		        ans.add(pod_ans);
			
			}
			rs.close();
			
			saveData(ans, "src\\main\\java\\resources\\output.json");
			statement.close();
			connection.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}

	// Статистика 1 (Студент): Общая статистика по успеваемости по предмету (лично/по группе/по потоку)	
	public static void AcademicPerformanceDisciplineStatistic(int student_id, int group_id, int discipline_id) {
		
		HashMap<Integer, Double> personal_statistics = new HashMap<>();
		personal_statistics = PersonalDisciplineStatistics(student_id, discipline_id);
		
		double sum_rainigs = 0;
		for (Map.Entry<Integer, Double> monthEntry : personal_statistics.entrySet()) 
			sum_rainigs += monthEntry.getValue();
		
		double exact_personal_reiting = sum_rainigs / personal_statistics.size();
		double personal_reiting = Math.round(exact_personal_reiting * 100.0) / 100.0;
		double group_persent_statistic = GroupDisciplineStatistics(exact_personal_reiting, student_id, discipline_id, group_id);
		double stream_persent_statistic = StreamDisciplineStatistics(exact_personal_reiting, student_id, discipline_id);
		
		ArrayList<String> ans = new ArrayList<>();
		ans.add(String.valueOf(personal_reiting));
		ans.add(String.valueOf(group_persent_statistic));
		ans.add(String.valueOf(stream_persent_statistic));
		
		saveData(ans, "src\\main\\java\\resources\\output.json");
		
	}
	
	public static HashMap<Integer, Double> PersonalDisciplineStatistics(int student_id, int discipline_id){
		
		Connection connection;
		Statement statement;
		ResultSet rs;
		
		HashMap<Integer, Double> ans = new HashMap<>();
	
		HashMap<Integer, HashMap<Integer, Integer>> results = new HashMap<>();
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select test_id, attempt_datetime, attempt_result from attempt join test using(test_id)"
					+ " where student_id = %d and discipline_id = %d"
					+ " order by test_id, attempt_datetime", student_id, discipline_id);
							
			rs = statement.executeQuery(query);
			
			while (rs.next()) {
				
				int test_id = rs.getInt("test_id");
				int attempt_month = rs.getDate("attempt_datetime").toLocalDate().getMonth().getValue();
				int attempt_result = rs.getInt("attempt_result");
				
				if (!results.containsKey(attempt_month)) {
					HashMap<Integer, Integer> map = new HashMap<>();
					map.put(test_id, attempt_result);
					results.put(attempt_month, map);
				}
				else {
					if (!results.get(attempt_month).containsKey(test_id)) {
						results.get(attempt_month).put(test_id, attempt_result);
					}
					else {
						if (results.get(attempt_month).get(test_id) < attempt_result) 
							results.get(attempt_month).put(test_id, attempt_result);
					}
				}
				
			}
			
			rs.close();
			statement.close();
			connection.close();
			
			for (Map.Entry<Integer, HashMap<Integer, Integer>> monthEntry : results.entrySet()) {
			    int month = monthEntry.getKey();
			    HashMap<Integer, Integer> testResults = monthEntry.getValue();

			    int sum = 0, count = 0;

			    for (Map.Entry<Integer, Integer> testEntry : testResults.entrySet()) {
			        int bestScore = testEntry.getValue();
			        sum += bestScore;
			        count++;
			    }
			    
			    ans.put(month, sum * 1.0 / count);
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ans;
		
	}

	public static double GroupDisciplineStatistics(double student_sum_raiting, int student_id, int discipline_id, int study_group_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs;
	
		ArrayList<Double> group_reitings = new ArrayList<>();
		int count_tests = 0, student_id_step = 0;
		double reiting = 0.0;
		boolean flag = false;
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select attempt_result, student_id from discipline join test using(discipline_id) join("
					+ " select a.attempt_result, a.test_id, a.student_id from attempt a"
					+ " where a.student_id <> %d and a.attempt_result = (select max(a2.attempt_result) from attempt a2"
					+ " where a2.student_id = a.student_id and a.test_id = a2.test_id)) as b using(test_id) join student using(student_id)"
					+ " where discipline_id = %d and study_group_id = %d"
					+ " order by student_id", student_id, discipline_id, study_group_id);
							
			rs = statement.executeQuery(query);
			
			while (rs.next()) {
				
				int attempt_result = rs.getInt("attempt_result");
				int another_student_id = rs.getInt("student_id");
				
				if (another_student_id != student_id_step) {
					if (!flag) {
						student_id_step = another_student_id;
						reiting += attempt_result;
						count_tests++;
						flag = true;
					}
					else {
						group_reitings.add(reiting / count_tests);
						student_id_step = another_student_id;
						reiting = attempt_result;
						count_tests = 1;
					}
				}
				else {
					reiting += attempt_result;
					count_tests++;
				}
				
			}
			
			rs.close();
			statement.close();
			connection.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		double count = 0;
		for (int i = 0; i < group_reitings.size(); i++) {
			if (group_reitings.get(i) < student_sum_raiting)
				count += 1.0;
		}
		
		return Math.round(count / group_reitings.size() * 10000.0)/100.0; 

	}

	public static double StreamDisciplineStatistics(double student_sum_raiting, int student_id, int discipline_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs;
	
		ArrayList<Double> group_reitings = new ArrayList<>();
		int count_tests = 0, student_id_step = 0;
		double reiting = 0.0;
		boolean flag = false;
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select attempt_result, student_id from discipline join test using(discipline_id) join("
					+ " select a.attempt_result, a.test_id, a.student_id from attempt a"
					+ " where a.student_id <> %d and a.attempt_result = (select max(a2.attempt_result) from attempt a2"
					+ " where a2.student_id = a.student_id and a.test_id = a2.test_id)) as b using(test_id)"
					+ " where discipline_id = %d"
					+ " order by student_id", student_id, discipline_id);
							
			rs = statement.executeQuery(query);
			
			while (rs.next()) {
				
				int attempt_result = rs.getInt("attempt_result");
				int another_student_id = rs.getInt("student_id");
				
				if (another_student_id != student_id_step) {
					if (!flag) {
						student_id_step = another_student_id;
						reiting += attempt_result;
						count_tests++;
						flag = true;
					}
					else {
						group_reitings.add(reiting / count_tests);
						student_id_step = another_student_id;
						reiting = attempt_result;
						count_tests = 1;
					}
				}
				else {
					reiting += attempt_result;
					count_tests++;
				}
				
			}
			
			rs.close();
			statement.close();
			connection.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		double count = 0;
		for (int i = 0; i < group_reitings.size(); i++) {
			if (group_reitings.get(i) < student_sum_raiting)
				count += 1.0;
		}
		
		return Math.round(count / group_reitings.size() * 10000.0)/100.0; 
	}
	
	// Статистика 2 (Студент): Общая статистика по успеваемости по тесту предмета (лично/по группе/по потоку)	
	public static void TestPerformanceStatistics(int student_id, int group_id, int test_id) {
		
		ArrayList<ArrayList<String>> ans = new ArrayList<>();
		ArrayList<String> pod_ans = new ArrayList<>();
		
		double double_personal_result = PersonalTestStatistics(student_id, test_id);
		String personal_result = String.valueOf(double_personal_result);
		String group_persent_result = String.valueOf(GroupTestStatistics(double_personal_result, student_id, test_id, group_id));
		String stream_persent_result = String.valueOf(StreamTestStatistics(double_personal_result, student_id, test_id));
		
		pod_ans.add(personal_result);
		pod_ans.add(group_persent_result);
		pod_ans.add(stream_persent_result);
		ans.add(pod_ans);
		pod_ans = new ArrayList<>();

		ArrayList<String> personal_answers_statistics = PersonalAnswerStatistics(student_id, test_id);
		ArrayList<String> group_answers_statistics = GroupAnswerStatistics(student_id, test_id, group_id);
		ArrayList<String> stream_answers_statistics = StreamAnswerStatistics(student_id, test_id);
		ans.add(personal_answers_statistics);
		ans.add(group_answers_statistics);
		ans.add(stream_answers_statistics);
		
		saveData(ans, "src\\main\\java\\resources\\output.json");
		
	}
	
	public static double PersonalTestStatistics(int student_id, int test_id){
		
		Connection connection;
		Statement statement;
		ResultSet rs;
		
		int ans = 0;
	
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select attempt_result from discipline join test using(discipline_id) join("
					+ " select a.attempt_result, a.test_id, a.student_id from attempt a"
					+ " where a.student_id = %d and a.attempt_result = (select max(a2.attempt_result) from attempt a2"
					+ " where a2.student_id = a.student_id and a.test_id = a2.test_id)) as b using(test_id) join student using(student_id)"
					+ " where test_id = %d", student_id, test_id);
							
			rs = statement.executeQuery(query);
			
			if (rs.next()) 
				ans = rs.getInt("attempt_result");
			
			rs.close();
			statement.close();
			connection.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ans * 1.0;
		
	}

	public static double GroupTestStatistics(double student_result, int student_id, int test_id, int study_group_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs;
		
		int count_results = 0, count_worse_results = 0;
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select attempt_result as results from discipline join test using(discipline_id) join("
					+ " select a.attempt_result, a.test_id, a.student_id from attempt a"
					+ " where a.student_id <> %d and a.attempt_result = (select max(a2.attempt_result) from attempt a2"
					+ " where a2.student_id = a.student_id and a.test_id = a2.test_id)) as b using(test_id) join student using(student_id)"
					+ " where test_id = %d and study_group_id = %d", student_id, test_id, study_group_id);
							
			rs = statement.executeQuery(query);
			
			while (rs.next()) {
				if (rs.getInt("results") < student_result)
					count_worse_results++;
				count_results++;
			}
			
			rs.close();
			statement.close();
			connection.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return Math.round(count_worse_results * 1.0 / count_results * 10000.0)/100.0;
	}
	
	public static double StreamTestStatistics(double student_result, int student_id, int test_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs;
		
		int count_results = 0, count_worse_results = 0;
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select attempt_result as results from discipline join test using(discipline_id) join("
					+ " select a.attempt_result, a.test_id, a.student_id from attempt a"
					+ " where a.student_id <> %d and a.attempt_result = (select max(a2.attempt_result) from attempt a2"
					+ " where a2.student_id = a.student_id and a.test_id = a2.test_id)) as b using(test_id)"
					+ " where test_id = %d", student_id, test_id);
							
			rs = statement.executeQuery(query);
			
			while (rs.next()) {
				if(rs.getInt("results") < student_result)
					count_worse_results++;
				count_results++;
			}
			
			rs.close();
			statement.close();
			connection.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return Math.round(count_worse_results * 1.0 / count_results * 10000.0)/100.0;
	}

	public static ArrayList<String> PersonalAnswerStatistics(int student_id, int test_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs;
		
		ArrayList<String> ans = new ArrayList<>();
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select case\r\n"
					+ " when result_per_question > 0 then 1 else 0 end as result_per_question from test_result "
					+ " where attempt_id in (select a.attempt_id from attempt a"
					+ " where a.test_id = %d and student_id = %d and a.attempt_datetime = (select min(a2.attempt_datetime)"
					+ " from attempt a2 where a2.student_id = a.student_id and a2.test_id = %d)"
					+ " order by a.student_id, a.attempt_datetime)", test_id, student_id, test_id);
							
			rs = statement.executeQuery(query);
			
			
			while (rs.next())
				ans.add(String.valueOf(rs.getInt("result_per_question") * 1.0));
			
			rs.close();
			statement.close();
			connection.close();
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return ans;
	}
	
	public static ArrayList<String> GroupAnswerStatistics(int student_id, int test_id, int study_group_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs;
		
		ArrayList<Double> ans = new ArrayList<>();
		ArrayList<String> str_ans = new ArrayList<>();
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select case"
					+ " when result_per_question > 0 then 1 else 0 end as result_per_question, attempt_id from test_result "
					+ " where attempt_id in"
					+ " (select a.attempt_id from student join attempt a using(student_id)"
					+ " where a.test_id = %d and study_group_id = %d and student_id <> %d and a.attempt_datetime = "
					+ " (select min(a2.attempt_datetime)"
					+ " from attempt a2 where a2.student_id = a.student_id and a2.test_id = %d)"
					+ " order by a.student_id, a.attempt_datetime)", test_id, study_group_id, student_id, test_id);
							
			rs = statement.executeQuery(query);
			
			boolean flag_attempt = false;
			int step_attempt_id = 0, count_questions = 0, count_repeat_questions = 0;
			
			while (rs.next()) {
				
				int result_per_question = rs.getInt("result_per_question");
				int attempt_id = rs.getInt("attempt_id");
				
				if (!flag_attempt) {
					flag_attempt = true;
					step_attempt_id = attempt_id;
				}
				
				if (step_attempt_id != attempt_id) {
					count_repeat_questions++;
					count_questions = 1;
					ans.set(count_questions - 1, ans.get(count_questions - 1) + result_per_question * 1.0);
					step_attempt_id = attempt_id;
				}
				else {
					count_questions++;
					if (ans.size() < count_questions)
						ans.add(result_per_question * 1.0);
					else
						ans.set(count_questions - 1, ans.get(count_questions - 1) + result_per_question * 1.0);
				}

			}
			
			rs.close();
			statement.close();
			connection.close();
			
			count_repeat_questions++;
			for(int i = 0; i < ans.size(); i++) {
				ans.set(i, Math.round(ans.get(i) / count_repeat_questions * 10000.0)/100.0);
				str_ans.add(String.valueOf(ans.get(i)));
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return str_ans;
	}
	
	public static ArrayList<String> StreamAnswerStatistics(int student_id, int test_id) {
		
		Connection connection;
		Statement statement;
		ResultSet rs;
		
		ArrayList<Double> ans = new ArrayList<>();
		ArrayList<String> str_ans = new ArrayList<>();
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select case"
					+ " when result_per_question > 0 then 1 else 0 end as result_per_question, attempt_id from test_result "
					+ " where attempt_id in (select a.attempt_id from attempt a"
					+ " where a.test_id = %d and a.attempt_datetime = (select min(a2.attempt_datetime)"
					+ " from attempt a2 where a2.student_id = a.student_id and a2.student_id <> %d and a2.test_id = %d)"
					+ " order by a.student_id, a.attempt_datetime)", test_id, student_id, test_id);
							
			rs = statement.executeQuery(query);
			
			boolean flag_attempt = false;
			int step_attempt_id = 0, count_questions = 0, count_repeat_questions = 0;
			
			while (rs.next()) {
				
				int result_per_question = rs.getInt("result_per_question");
				int attempt_id = rs.getInt("attempt_id");
				
				if (!flag_attempt) {
					flag_attempt = true;
					step_attempt_id = attempt_id;
				}
				
				if (step_attempt_id != attempt_id) {
					count_repeat_questions++;
					count_questions = 1;
					ans.set(count_questions - 1, ans.get(count_questions - 1) + result_per_question * 1.0);
					step_attempt_id = attempt_id;
				}
				else {
					count_questions++;
					if (ans.size() < count_questions)
						ans.add(result_per_question * 1.0);
					else
						ans.set(count_questions - 1, ans.get(count_questions - 1) + result_per_question * 1.0);
				}

			}
			
			rs.close();
			statement.close();
			connection.close();
			
			count_repeat_questions++;
			for(int i = 0; i < ans.size(); i++) {
				ans.set(i, Math.round(ans.get(i) / count_repeat_questions * 10000.0)/100.0);
				str_ans.add(String.valueOf(ans.get(i)));
			}
			
		} catch (SQLException e) {
			e.printStackTrace();
		}

		
		
		return str_ans;
	}
	
	// Личный кабинет (Студент): Накопленный рейтинг	
	public static void AccumulatedRating(int student_id) {
			
		Connection connection;
		Statement statement;
		ResultSet rs;
		
		try {
			
			connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD);
			statement = connection.createStatement();

			String query = String.format("select attempt_result, discipline_id from discipline join test using(discipline_id) join("
					+ " select a.attempt_result, a.test_id from attempt a"
					+ " where a.student_id = %d and a.attempt_result = (select max(a2.attempt_result) from attempt a2"
					+ " where a2.student_id = a.student_id and a.test_id = a2.test_id)) as b using(test_id)"
					+ " order by discipline_id", student_id);
							
			rs = statement.executeQuery(query);

			int number_discipline = 0, second_number_discipline = number_discipline, sum_results = 0, count_tests = 0;
			ArrayList<Double> results = new ArrayList<>();
			boolean first_flag = false;
			
			int discipline_id, attempt_result;
			
			while (rs.next()) {
				
				discipline_id = rs.getInt("discipline_id");
				attempt_result = rs.getInt("attempt_result");
				
				if (!first_flag) {
					number_discipline = discipline_id;
					first_flag = true;
				}
				second_number_discipline = discipline_id;
				
				if (number_discipline != second_number_discipline) {
					results.add(sum_results * 1.0 / count_tests);
					sum_results = attempt_result;
					count_tests = 1;
				}
				else {
					sum_results += attempt_result;
					count_tests ++;
				}
				
				number_discipline = second_number_discipline;
				
			}
			
			results.add(sum_results * 1.0 / count_tests);
			
			rs.close();
			statement.close();
			connection.close();
			
			double sum_average_results = 0, accumulated_rating;
			
			for(int i = 0; i < results.size(); i++) 
				sum_average_results += results.get(i);
			
			accumulated_rating = Math.round(sum_average_results / results.size() * 100.0) / 100.0;
			
			saveData(accumulated_rating, "src\\main\\java\\resources\\output.json");
//			System.out.println(accumulated_rating);
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
	}	

	@SuppressWarnings("unchecked")
	public static void saveData(Object data, String filePath) {
        JSONArray jsonArray = new JSONArray();

        if (data instanceof Double) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("number", data);
            jsonArray.put(jsonObject);
        } else if (data instanceof List) {
            List<Object> list = (List<Object>) data;
            for (Object item : list) {
                if (item instanceof List) {
                    JSONArray innerListArray = new JSONArray();
                    for (Object innerItem : (List<Object>) item) {
                        innerListArray.put(innerItem.toString());
                    }
                    jsonArray.put(innerListArray);
                } else {
                    jsonArray.put(item.toString());
                }
            }
        }

        try (FileWriter fileWriter = new FileWriter(filePath)) {
            fileWriter.write(jsonArray.toString());
        } catch (IOException e) {
            System.out.println("Error writing to file: " + e.getMessage());
        }
    }
}