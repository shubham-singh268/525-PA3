base = buffer_mgr.o buffer_mgr_stat.o dberror.o expr.o rm_serializer.o storage_mgr.o 

test_expr : $(base) test_expr.o
	gcc -o test_expr $(base) test_expr.o
	rm *.o

test : $(base) test_assign3_1.o
	gcc -o test $(base) test_assign3_1.o
	rm *.o

buffer_mgr.o : buffer_mgr.c
	gcc -c buffer_mgr.c -I .

buffer_mgr_stat.o : buffer_mgr_stat.c
	gcc -c buffer_mgr_stat.c -I .

dberror.o : dberror.c
	gcc -c dberror.c -I .

expr.o : expr.c
	gcc -c expr.c -I .

rm_serializer.o : rm_serializer.c
	gcc -c rm_serializer.c -I .

storage_mgr.o : storage_mgr.c
	gcc -c storage_mgr.c -I .

test_expr.o : test_expr.c
	gcc -c test_expr.c -I .

test_assign3_1.o : test_assign3_1.c
	gcc -c test_assign3_1.c -I .

.PHONY : clean
clean :
	rm test_expr test
