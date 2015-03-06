package derby.JPAStudentInfo;

import javax.persistence.*;

@Entity
public class Enroll {

    @Id private int eid;
    private String grade = null;

    @ManyToOne
    @JoinColumn(name="StudentId")
    private derby.StudentInfo.Student student;

    @ManyToOne
    @JoinColumn(name="SectionId")
    private derby.JPAStudentInfo.Section section;

	public Enroll() {}

	public Enroll(int eid, derby.StudentInfo.Student student, derby.JPAStudentInfo.Section section) {
		this.eid = eid;
		this.student = student;
		this.section = section;
	}

    public int getId() {
        return eid;
    }

    public derby.StudentInfo.Student getStudent() {
        return student;
    }

	public derby.JPAStudentInfo.Section getSection() {
        return section;
    }

    public String getGrade() {
		return grade;
	}

	public void changeGrade(String grade) {
		this.grade = grade;
	}
}
