create table t(a int, b int, c int);
insert into t(a, b, c) values(10, 20, 30);
insert into t(a, c) values(40, 50);
insert into t(b) values(60);
select * from t;

create table t2(a int, b varchar, c int);
insert into t2(a, b, c) values(1, 'abc', 2);
insert into t2(b) values('def');
insert into t2(a) values(3);
insert into t2(c) values(4);
select * from t2;

