#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#LAB 1 - PYTHON SUBMISSION


# In[2]:


import mysql.connector
import pandas as pd


# In[2]:


connection = mysql.connector.connect(host = "localhost",
                                     user = "root",
                                     password = "@Test123$")

cursorObject = connection.cursor()


# In[4]:


connection = mysql.connector.connect(host ="localhost",
                                     user ="root",
                                     passwd ="@Test123$")
 
cursorObject = connection.cursor()
 
cursorObject.execute("CREATE DATABASE E_Commerce1")

connection.close()


# In[7]:


connection = mysql.connector.connect(host ="localhost",
                                     user ="root",
                                     passwd ="@Test123$",
                                     database = 'e_commerce1')
 

cursorObject = connection.cursor()

tables=""" create table supplier(SUPP_ID int primary key, 
                                        SUPP_NAME varchar(50),
                                        SUPP_CITY varchar(50), 
                                        SUPP_PHONE varchar(10));

                  create table customer (CUS_ID int primary key, 
                                         CUS_NAME VARCHAR(20) NULL DEFAULT NULL,
                                         CUS_PHONE VARCHAR(10),
                                         CUS_CITY varchar(30) ,
                                         CUS_GENDER CHAR);
                                         

                  create table category (CAT_ID int  primary key,
                                         CAT_NAME VARCHAR(20) NULL DEFAULT NULL);

                  create table product (PRO_ID int PRIMARY KEY , 
                                        PRO_NAME VARCHAR(20) NULL DEFAULT NULL,
                                        PRO_DESC VARCHAR(60) NULL DEFAULT NULL, 
                                        CAT_ID INT NOT NULL,
                                        FOREIGN KEY (CAT_ID) REFERENCES category (CAT_ID));

                  create table product_details (PROD_ID int primary key,
                                                 PRO_ID INT NOT NULL, 
                                                 SUPP_ID INT NOT NULL, 
                                                 PROD_PRICE INT NOT NULL, 
                                                 FOREIGN KEY (PRO_ID) REFERENCES product (PRO_ID), 
                                                 FOREIGN KEY (SUPP_ID) REFERENCES supplier(SUPP_ID));
                                                 
                  create table orders (ORD_ID int primary key, 
                                      ORD_AMOUNT INT NOT NULL , 
                                      ORD_DATE DATE, 
                                      CUS_ID INT NOT NULL, 
                                      PROD_ID INT NOT NULL,
                                      FOREIGN KEY (CUS_ID) REFERENCES customer(CUS_ID),
                                      FOREIGN KEY (PROD_ID) REFERENCES product_details(PROD_ID));

                  create table rating (RAT_ID int primary key, 
                                      CUS_ID INT NOT NULL, 
                                      SUPP_ID INT NOT NULL, 
                                      RAT_RATSTARS INT NOT NULL,
                                      FOREIGN KEY (SUPP_ID) REFERENCES supplier (SUPP_ID),
                                      FOREIGN KEY (CUS_ID) REFERENCES customer(CUS_ID));
                                      """

cursorObject.execute(tables)


# In[10]:


## closing the connection 
connection.close()
## Lets make a connection to Mysql server and choose database 'e_commerce'
connection = mysql.connector.connect(host ="localhost",
                                     user ="root",
                                     passwd ="@Test123$",
                                     database='e_commerce1')
## creating a cursor object
cursorObject = connection.cursor()


# In[11]:


insert_supplier = """INSERT INTO supplier(SUPP_ID, SUPP_NAME, SUPP_CITY, SUPP_PHONE)
                    VALUES(%s,%s,%s,%s)"""

val = [(1,'Rajesh Retails','Delhi',1234567890),
      (2,'Appario Ltd.','Mumbai',2589631470),
      (3,'Knome products','Bangalore',9785462315),
      (4,'Bansal Retails','Kochi',8975463285),
      (5,'Mittal Ltd.','Lucknow',7898456532)]

cursorObject.executemany(insert_supplier,val)
connection.commit()


# In[12]:


insert_customer_details="""INSERT INTO customer(CUS_ID , CUS_NAME, CUS_PHONE, CUS_CITY ,CUS_GENDER)
                    VALUES(%s,%s,%s,%s,%s)"""
val=[(1,'AAKASH',9999999999,'DELHI','M'),
     (2,'AMAN',9785463215,'NOIDA','M'),
     (3,'NEHA',9999999998,'MUMBAI','F'),
     (4,'MEGHA',9994562399,'KOLKATA','F'),
     (5,'PULKIT',7895999999,'LUCKNOW','M')]
cursorObject.executemany(insert_customer_details,val)
connection.commit()


# In[13]:


insert_category="""INSERT INTO category(CAT_ID , CAT_NAME) 
                    VALUES(%s,%s)"""
val=[(1,'BOOKS'),
     (2,'GAMES'),
     (3,'GROCERIES'),
     (4,'ELECTRONICS'),
     (5,'CLOTHES')]
cursorObject.executemany(insert_category,val)
connection.commit()


# In[14]:


insert_product_details="""INSERT INTO product(PRO_ID,PRO_NAME, PRO_DESC,CAT_ID )
                    VALUES(%s,%s,%s,%s)"""
val=[(1,'GTA V','DFJDJFDJFDJFDJFJF',2),
     (2,'TSHIRT','DFDFJDFJDKFD',5),
     (3,'ROG LAPTOP','DFNTTNTNTERND',4),
     (4,'OATS','REURENTBTOTH',3),
     (5,'HARRY POTTER','NBEMCTHTJTH',1)]


cursorObject.executemany(insert_product_details,val)
connection.commit()


# In[15]:


insert_products="""INSERT INTO product_details(PROD_ID,PRO_ID,SUPP_ID,PROD_PRICE )
                    VALUES(%s,%s,%s,%s)"""
val=[(1,1,2,1500),
     (2,3,5,30000),
     (3,5,1,3000),
     (4,2,3,2500),
     (5,4,1,1000)]


cursorObject.executemany(insert_products,val)
connection.commit()


# In[16]:


insert_orders="""INSERT INTO orders(ORD_ID, ORD_AMOUNT,ORD_DATE,CUS_ID,PROD_ID )
                    VALUES(%s,%s,%s,%s,%s)"""
val=[(20,1500,'2021-10-12',3,5),
     (25,30500,'2021-09-16',5,2),
     (26,2000,'2021-10-05',1,1),
     (30,3500,'2021-08-16',4,3),
     (50,2000,'2021-10-06',2,1)]

cursorObject.executemany(insert_orders,val)
connection.commit()


# In[17]:


insert_rating="""INSERT INTO rating(RAT_ID,CUS_ID,SUPP_ID, RAT_RATSTARS)
                    VALUES(%s,%s,%s,%s)"""
val=[(1,2,2,4),
     (2,3,4,3),
     (3,5,1,5),
     (4,1,3,2),
     (5,4,5,4)]

cursorObject.executemany(insert_rating,val)
connection.commit()


# In[32]:


connection.close()


# In[33]:


connection = mysql.connector.connect(host ="localhost",
                                     user ="root",
                                     passwd ="M@njali4194",
                                     database='e_commerce1')
## creating a cursor object
cursorObject = connection.cursor()


# In[34]:


question_one=""" select c.CUS_GENDER,count(c.CUS_ID) from customer c 
                left join 
                orders o on o.CUS_ID=c.CUS_ID where
                ORD_AMOUNT>=3000 group by CUS_GENDER;
             """

cursorObject.execute(question_one)
 
output = cursorObject.fetchall()
output_df = pd.DataFrame(output, columns=['Customer-name','Count of the customers'])
output_df


# In[35]:


question_two="""select o.ORD_ID,o.ORD_AMOUNT,o.ORD_DATE,o.CUS_ID,o.PROD_ID,p.PRO_NAME from orders o 
    join product_details pd on o.PROD_ID=pd.PROD_ID 
    join product p on p.PRO_ID=pd.PRO_ID group by CUS_ID having CUS_ID=2;
    """
cursorObject.execute(question_two)
 
output = cursorObject.fetchall()
output_df = pd.DataFrame(output, columns=['Order_Id','Order_Amount','Order_Date','Customer_Id','Product_Id','Product_Name'])
output_df


# In[36]:


question_three=""" select  s.SUPP_ID ,s.SUPP_NAME,s.SUPP_CITY,s.SUPP_PHONE from supplier s  inner join 
                 ( select pd.SUPP_ID from product_details pd group by 
                 SUPP_ID having count(SUPP_ID)>1) as c on s.SUPP_ID=c.SUPP_ID;';
             """

cursorObject.execute(question_three)
 
output = cursorObject.fetchall()
output_df = pd.DataFrame(output, columns=['Supplier_Id','Supplier_Name','Supplier_City','Supplier_Phone'])
output_df


# In[38]:


connection.close()


# In[39]:


connection = mysql.connector.connect(host ="localhost",
                                     user ="root",
                                     passwd ="M@njali4194",
                                     database='e_commerce1')
## creating a cursor object
cursorObject = connection.cursor()


# In[40]:


question_four=""" select o.ORD_AMOUNT,c.CAT_ID,c.CAT_NAME from orders o
                   join product_details pd on o.PROD_ID=pd.PROD_ID 
                   join product p on p.PRO_ID=pd.PRO_ID
                   join category c on c.CAT_ID=p.PRO_ID having min(o.ORD_AMOUNT);
             """

cursorObject.execute(question_four)
 
output = cursorObject.fetchall()
output_df = pd.DataFrame(output, columns=['Order_Amount','Category_Id','Category_Name'])
output_df


# In[41]:


question_five="""select p.PRO_ID,p.PRO_NAME,o.ORD_DATE from orders o 
                 inner join product_details pd on pd.PROD_ID=o.PROD_ID 
                 inner join product p on p.PRO_ID=pd.PRO_ID 
                 having o.ORD_DATE>'2021-10-05';
                 """
cursorObject.execute(question_five)
 
output = cursorObject.fetchall()
output_df = pd.DataFrame(output, columns=['Product_ID','Product_name','Order_Date'])
output_df


# In[42]:


question_six="""select s.SUPP_ID,s.SUPP_NAME,c.CUS_NAME,r.RAT_RATSTARS from supplier s
                join rating r on r.SUPP_ID=s.SUPP_ID
                join customer c on c.CUS_ID=r.CUS_ID order by r.RAT_RATSTARS DESC limit 3;
                 """
cursorObject.execute(question_six)
 
output = cursorObject.fetchall()
output_df = pd.DataFrame(output, columns=['Supplier_Id','Supplier_Name','Customer_Name','Rating'])
output_df


# In[43]:


question_seven=""" select CUS_NAME,CUS_GENDER from customer where CUS_NAME LIKE '%A%';
             """

cursorObject.execute(question_seven)
 
output = cursorObject.fetchall()
output_df = pd.DataFrame(output, columns=['Customer_name','Customer_gender'])
output_df


# In[44]:


question_eight="""select sum(o.ORD_AMOUNT) from orders o inner join customer c on c.CUS_ID=o.CUS_ID where c.CUS_GENDER='M';"""

cursorObject.execute(question_eight)
 
output = cursorObject.fetchall()
output_df = pd.DataFrame(output, columns=['Total_Order_Amount'])
output_df


# In[45]:


question_nine=""" select c.CUS_ID,c.CUS_NAME,c.CUS_PHONE,c.CUS_CITY,c.CUS_GENDER from customer c
                  left outer join orders o on c.CUS_ID=o.CUS_ID ;
              """

cursorObject.execute(question_nine)
 
output = cursorObject.fetchall()
output_df = pd.DataFrame(output, columns=['Customer_Id','Customer_Name','Customer_Phone','Customer_City','Customer_Gender'])
output_df


# In[46]:


connection.close()


# In[ ]:




