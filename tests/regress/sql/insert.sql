create table t(a int, b int, c int);
insert into t(a, b, c) values(10, 20, 30);
insert into t(a, c) values(40, 50);
insert into t(b) values(60);
select * from t;

