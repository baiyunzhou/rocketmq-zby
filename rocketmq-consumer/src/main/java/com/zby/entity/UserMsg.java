package com.zby.entity;

import java.util.Date;

public class UserMsg {

	private Long id;
	private String name;
	private Date birthday;

	public UserMsg() {
		super();
	}

	public UserMsg(Long id, String name, Date birthday) {
		super();
		this.id = id;
		this.name = name;
		this.birthday = birthday;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getBirthday() {
		return birthday;
	}

	public void setBirthday(Date birthday) {
		this.birthday = birthday;
	}

	@Override
	public String toString() {
		return "UserMsg [id=" + id + ", name=" + name + ", birthday=" + birthday + "]";
	}

}
