package derby.JPAStudentInfo;

import javax.persistence.*;

@Entity
public class Permit {

    @Id private int pid;
    private String licensePlate;
    private String carModel;

    @OneToOne
    @JoinColumn(name="StudId")
    private derby.StudentInfo.Student student;

	public Permit() {}

	public Permit(int pid, String licensePlate, String carModel, derby.StudentInfo.Student s) {
		this.pid = pid;
		this.licensePlate = licensePlate;
		this.carModel = carModel;
		student = s;
	}

    public int getId() {
        return pid;
    }

    public String getPlate() {
        return licensePlate;
    }

    public void changePlate(String newplate) {
        licensePlate = newplate;
    }

	public String getCarModel() {
        return carModel;
    }

    public void changeCarModel(String newmodel) {
        carModel = newmodel;
    }

    public derby.StudentInfo.Student getStudent() {
		return student;
	}
}
