---
layout: post
title: "Using Azure Data Factory to analyze a Cars dataset"
date: 2021-03-14
category: Azure
published: True
---



Azure Data Factory can be used for various ETL/ELT and Data integration scenarios. Here, I am using it to process and transform data and publish it to Azure Data Lake Storage.  

["Car details from Car Dekho.csv"](https://www.kaggle.com/nehalbirla/vehicle-dataset-from-cardekho) file contains information about used cars. I use this dataset to find the cars that are value for money and are well suited for ones liking.

To achieve this, I curate a list of cars according to a user's choice, whether one want a First/Second/Third/Fourth & Above Owner Car or a Test Drive Car and whether a user wants a manual or automatic transmission car. I pass these choices as parameters to my pipeline. I chose to analyze only petrol cars.

To ensure that a car is value for money, I filter out only the cars that have a selling price less than the average selling price and the kilo-meters driven less than the average kilo-meters driven.

## <u>Setup</u>

You Must have a ADLS storage account setup. [This link will help you with it](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal). To store your files. create 2 container. I named one of them as *source-etl* and the other as *sink-etl*. Upload your dataset to the *source-etl* container.

Create a data factory. [Link to the setup](https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-portal#create-a-data-factory).

## <u>Creating pipelines</u>

We will be creating two pipelines. 

1. In the Data Factory UI, select **create pipeline**. In the **general** tab (on the right-side of the screen) Enter the *Carspipeline* as the name of your pipeline.

2. From the activities tab, under **Move & transform** , drag the **data flow** activity to the pipeline canvas.

3. A pop-up will appear, in its **general** tab enter *CarsTransform* as the name. 

4. In the **settings** tab, select **New** data flow. A new Data Flow window will open. This is the data flow canvas where we will describe our activity. Name it *Carsdataflow*. 

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/data%20flow%20setup1.PNG?raw=true" alt="image">
   </html>

5. Go back to the pipeline canvas and from the activities tab, under **General**, drag the **Execute pipeline** activity to the pipeline canvas. 

6. To create another pipeline, right-click on **Pipelines**, under the **Factory Resources** pane to create a new pipeline. Name it  *RecommendCarspipeline*. Drag and drop the **data flow** activity on the pipeline canvas and name it *RecommendCarsDataflow*.

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:auto;;height:100%;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/Factory%20resources.PNG?raw=true" alt="image">
   </html>

7. Now, go back to the *Carspipeline*, click on *CarsTransform*. Click and hold on the green tip of the activity and stretch it to the Execute pipeline activity. 

8. On selecting the Execute pipeline activity, a pop-up will appear. Under the **General** tab, fill the name box as *Execute RecommendCarspipeline* and select the *RecommendCarspipeline* for the **Invoked pipeline** option, under the **Settings** tab. Check the **Wait on completion** checkbox.

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/execute%20pipeline.PNG?raw=true" alt="image">
   </html>

## <u>Building Transformation logic for Carsdataflow</u>

Here, we'll build a data flow that takes the "Car details from Car Dekho.csv" in ADLS storage, finds the average selling price, average kilometer driven, total number of cars available and the total number of unique cars available for each owner-type. Then we'll write this file back to the ADLS storage.

Before doing anything, slide the **data flow debug** slider on. Debug mode allows for interactive testing of transformation logic against a live Spark cluster. You will be prompted to select which integration runtime configuration you wish to use. Be aware of the hourly charges incurred by Azure Databricks during the time that you have the debug session turned on.

<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/debug%20run.PNG?raw=true" alt="image">
</html>

1. In the canvas, click on **add source**. Under the **Source Settings** enter *ADLSsource* as the Output stream name. 

2. Select *DelimitedText* as the **source type**.

3. For linked service, click on **New**. A creation screen will open, name your linked service as *ADLSlinkedService*. Select *Azure data Lake Storage Gen2* as **type**. Select your azure **subscription** and your **Storage account name**. Click on create.

4. In the **Source options** tab, select *File* as **File mode**, enter the **File path** and check the **First row as header** checkbox.

5. In the **Data preview** tab, click on *refresh*. Your should be able to see a sample data.

6. In **Projection** tab, select *Import schema*. Your will be prompted to select the default formats of each datatype. After doing the needful, you will be able to see the schema. For this scenario, the schema doesn't need any correction.

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/ADLSsource%20schema.PNG?raw=true" alt="image">
   </html>

7. In the data flow canvas, click on the plus icon near the source node, to add a new transformation. Select **Filter**. 

8. Fill in the details according to the image below. 

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/YearFuelFilter.PNG?raw=true" alt="image">
   </html>

   When you click on the expression box, a expression builder window opens up. Here, you can write the expression as :

   `year >= (toInteger(year(currentDate())-6)) && rlike(fuel,'Petrol')`

   This statement simply means that I want to have only the cars that are not more that 6 years old and run on petrol.

   I used the `currentDate()` function to get the current date. Then I used the `year()` function to extract the year from the date and convert it to integer using the `toInteger()` function. To find the petrol cars, I used the `rlike()` function to find the pattern 'Petrol' in the column *fuel*. 

   you can verify your logic by clicking **Refresh** under the **preview** section, to see its output compared to the inputs used.

9. In the canvas, click on the plus icon for the filter node and add the **Aggregate** transformation.

10. Fill in the details according to the below image.

    <html>
    <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/OwnerAggregate1.PNG?raw=true" alt="image">
    </html>

    <html>
    <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/OwnerAggregate2.PNG?raw=true" alt="image">
    </html>

    As shown, add each column and write their respective aggregating functions.

    I used the `avg()` to find the average of a numerical column, `count()` to count the number of rows and `countDistinct()` to count the unique values in a column.

11. Next, add the **ModifyColumns** node and fill the details accordingly. We use the `round()` function to round-off the average values as high level of precision is not needed.

    <html>
    <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/RoundoffModifyCols.PNG?raw=true" alt="image">
    </html>

12. In the end, add a sink node. Fill the sink tab according to the image below. In the **Settings** tab, check the **first row as header** checkbox, select *Output to single file* in the **File name option** and name the **Output to single file** as *CarsOutput.csv*. These changes will prompt you to change the **Partition option** to *single partition* in the **Optimize** tab. 

    <html>
    <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/Sink.PNG?raw=true" alt="image">
    </html>

13. At last, you can click on **publish all** to save your pipeline. You can also debug a pipeline before you publish it. While data preview doesn't write data, a debug run will write data to your sink destination. This debug run will also try to trigger the *RecommendCarspipeline*, which is not complete yet and may throw an error.

In the end, you will get something like the below image.

<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/Cardataflow.PNG?raw=true" alt="image">
</html>

## <u>Building Transformation logic for RecommendCarsdataflow</u>

Here, we will join the original dataset with the output from the previous pipeline after some modification and find the cars that are value for money.

1. Create a **source** node, with the following details.

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/RecommendpipelineSource1.png?raw=true" alt="image">
   </html>

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/RecommendpipelineSource1.1.png?raw=true" alt="image">
   </html>

2. Next add a **filter** node and name it *FuelFilter*. Use the **Filter on** option to get only the "Petrol" cars. Write `fuel == "Petrol"` in the expression box.

3. Then add a **select** node and name it *RemoveFuelSellertype*. Make sure the **incoming stream** is *FuelFilter*. Let all the **Options** be checked. From the **Input columns** section delete the *Fuel* and *seller_type* column by clicking on the delete icon beside those columns.

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/RemoveFuelSellertype.PNG?raw=true" alt="image">
   </html>

4. Now add another **source** node with the following details.

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/CarsOutputsource1.PNG?raw=true" alt="image">
   </html>

   <html>

   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/CarsOutputsource2.PNG?raw=true" alt="image">
   </html>

5. Add a **Join** node to the **RemoveFuelSellertype** node. Fill the details according to the images below.

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/Ownerjoin.PNG?raw=true" alt="image">
   </html>

6. Now add a **filter** node to **OwnerJoin**. Click on the expression box for the **Filter on** option and open the **Visual expression builder**.

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/ValueforMoneyFilter.PNG?raw=true" alt="image">
   </html> 

   Under **Expression elements**, select **Parameters** and click on create new to add *Owner* and *type_of_transmission* as parameters. Make both of them as string datatype and add "First Owner" as default value to *Owner* and "Manual" to *type_of_transmission*. Then, add the following code to the expression box:

   `(selling_price <= avg_selling_price) && (km_driven <= avg_km_driven) && rlike(transmission, $type_of_transmission) && rlike(CarsOutputsource@owner, $Owner)`  

   This will extract only the rows where the selling-price is less than the average selling-price, the kilometer driven is less than the average kilometer driven, transmission is equal to the value of *type_of_transmission* parameter and owner is equal to the value of *owner* parameter.

7. Add a **Select** node and delete the column that do not appear in the below image.

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/RelavantColsSelect.PNG?raw=true" alt="image">
   </html>

8. Now add the **sort** node. Fill-in with the details below.

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/YearPriceSort.PNG?raw=true" alt="image">
   </html>

9. In the end add a **sink** node and fill in with required details.

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/RecommendListsink.PNG?raw=true" alt="image">
   </html>

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/RecommendListsink2.PNG?raw=true" alt="image">
   </html>

   <html>
   <img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/RecommendListsink3.PNG?raw=true" alt="image">
   </html>

Finally, your data flow should look similar to the below image.

<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/RecommendCars%20data%20flow.PNG?raw=true" alt="image">
</html>

To change the value of the your parameters, go back to the *RecommendCarspipeline* and click on the **Parameters** tab. Click on the **value** field for any of the parameters and select **pipeline expression** and write the static value you want for that parameter.

<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/recommendcarsspipeline%20parameter.PNG?raw=true" alt="image">
</html>

Click **Publish all** button and save your pipeline.

To trigger the *Carspipeline*, click on **Add trigger** and select **Trigger now**. You don't have to trigger *RecommendCarspipeline* separately as the pipeline execute activity will trigger it automatically.

<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:auto;;height:a100%;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/trigger%20now.PNG?raw=true" alt="image">
</html>

I ran the pipelines with the parameter *Owner* as "First Owner" and *type_of_transmission* as "Automatic".

You can check the status of execution from the **Monitor** tab. Click on a specific pipeline, to monitor it in-depth. The below image shows the 2 pipelines in progress.

<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/monitor%20pipelines.PNG?raw=true" alt="image">
</html>

Note the duration for each pipeline in the below image.

<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/monitor%20pipelines%20succeeded.PNG?raw=true" alt="image">
</html>

When you click on *Carspipeline*, you can examine it in more detail.

<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/manage%20carspieline.PNG?raw=true" alt="image">
</html>

The below image shows the processing time for each transformation of *CarsTransform* activity.

<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/monitor%20carstransform.PNG?raw=true" alt="image">
</html>

The image given below shows the processing time for each transformation of *RecommendCarsData flow* activity.

<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/time%20consumption%20recommendcars%20data%20flow.PNG?raw=true" alt="image">
</html>

After you are satisfied with your work and downloaded all the output files, you can go ahead and delete all the resources in order to save money. 

To view your files, you can open your storage account (as shown below).

<html>
<img style="display:block;margin-left:auto;margin-right:auto;width:100%;;height:auto;" src="https://github.com/TakreemAkhter/TakreemAkhter.github.io/blob/2760f7641a3eb7b03f996380da02fa9ec29aeba5/assets/images/storage%20account%20sink.PNG?raw=true" alt="image">
</html>