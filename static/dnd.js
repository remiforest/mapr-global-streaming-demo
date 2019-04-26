var SOURCE_POLL_RATE = 0.5 // poll source streams every xx seconds
var TARGET_POLL_RATE = 0.5 // poll target streams every xx seconds


// Plugin call for drag and drop
$(function(){
    $('.drag').draggable({revert:true,revertDuration: 0});
});



// AJAX call to deploy a new country
$('.drop').droppable({
  drop : function(e){
        var zone = $(this).attr('id');
        $.ajax({
            url: 'deploy_new_country',
            type: 'post',
            data: {"country":zone},
            success:function(data){
                $("#"+zone+"_docker_image").show();
            }
        });
    }
});


// AJAX call to remove a deployed country
$('#recycle').droppable({
  drop : function(e,ui){
        var zone = ui.draggable.attr('id').split("_")[0];
        $("#"+zone+"_docker_image").hide();
        $.ajax({
              url: 'remove_country',
              type: 'post',
              data: {"country":zone},
              success:function(data){
                $("#"+zone+"_chart").remove();
              }
          });
    }
});



// AJAX call to get the list of active streams and display the stream images if a stream exists
function display_streams(){
    $.ajax({
        url: 'get_streams',
        type: 'post',
        success:function(data){
            var current_streams = JSON.parse(data);
            // Parse all streams
            $(".stream").each(function(){
                // Displays if in current_streams
                var stream_name = $(this).data("stream_name");
                if(current_streams.includes(stream_name)){
                    $("#" + stream_name + "_stream").show();
                    if(!stream_name.includes("replica")) $("#" + stream_name + "_docker_image").show();
                }else{
                    $("#" + stream_name + "_stream").hide();
                    if(!stream_name.includes("replica")) $("#" + stream_name + "_docker_image").hide();
                }
            })
            if (6>current_streams.length && current_streams.length>0){
              $("#replicate_streams").show();
            }else{
              $("#replicate_streams").hide();
            }
            setTimeout(function(){
                display_streams();
              }, 1000);
        }
    });
}



$("#replicate_streams").click(function(){
    $("#replicate_streams").text("Replicating ...");
    $.ajax({
        url: 'replicate_streams',
        type: 'get',
        success:function(data){
            setTimeout(function(){
              $("#replicate_streams").text("Replicate streams");
              $("#global_streams").show();
            }, 1000 * 10);
          }
    });
});




function create_chart(country){
    var chart_div = country+"_chart";
    var myChart = Highcharts.chart(chart_div, {
        chart: {
            type: 'column',
            height : 200,
            width : 300,
        },
        title: {
            text: country
        },
        xAxis: {
            categories: [],
            labels:
              {
                enabled: false
              }
        },
        yAxis: {
            title: {
                text: 'Count'
            }
        },
        series: [{
            showInLegend: false, 
            data: []
        }]
    });
    return myChart;
}


function update_country_charts(sum){ // sum = True --> sum incoming and existing values, False --> incomming values only
    $.ajax({
        url: 'get_streams_data',
        type: 'post',
        data: {"scope":"source"},
        success:function(data){
            // data contains a dict
            // each stream is a key
            // value is a dict of car_model : car_count

            loaded_data = JSON.parse(data);
            
            // Iterate over incoming dictionary
            $.each(loaded_data,function(key,value){

                // get chart for the "key" stream
                var chart=$("#" + key + "_chart").highcharts();
                
                // get current chart values
                var chart_data = chart.series[0].data;

                // calculate new values
                // new_data is a dictionary of car_model : count
                var new_data = {};

                // first update the count for existing car models
                for (var i = 0; i < chart_data.length; i++) {
                    car_model = chart_data[i].category;
                    current_value = chart_data[i].y;
                    incoming_value = value[car_model];
                    if (incoming_value){
                        if(sum){
                            new_value = current_value + incoming_value;
                        }else{
                            new_value = incoming_value;
                        }
                    }else{
                        if(sum){
                            new_value = current_value;
                        }else{
                            new_value = 0;
                        }
                    }
                    new_data[car_model] = new_value;
                    delete value[car_model];
                }

                // then get the count for remaining car models that were not in the chart 
                for (var car_model in value){
                        new_data[car_model]=value[car_model];                        
                }

                // convert the dict into an array of objects
                var new_data_array = []
                for(var car_model in new_data){
                    new_data_array.push({"car_model":car_model,"count":new_data[car_model]})
                }

                // sort the new data array
                new_data_array.sort(function(a,b){
                    var x = a["car_model"]; var y = b["car_model"];
                    return ((x < y) ? -1 : ((x > y) ? 1 : 0));
                })

                // transform to two arrays (for x and y)
                var categories = []
                var data = []
                for (var k = 0; k < new_data_array.length; k++) {
                    categories[k] = new_data_array[k]["car_model"];
                    data[k] = new_data_array[k]["count"];
                }

                // update chart data
                chart.xAxis[0].setCategories(categories);
                chart.series[0].setData(data);  
            })
            
            setTimeout(function(){
                update_country_charts(sum);
              }, SOURCE_POLL_RATE * 1000);
        }
    });
}

function update_global_chart(sum){ // sum = True --> sum incoming and existing values, False --> incomming values only
    $.ajax({
        url: 'get_streams_data',
        type: 'post',
        data: {"scope":"target"},
        success:function(data){
            // data contains a dict
            // each stream is a key
            // value is a dict of car_model : car_count

            loaded_data = JSON.parse(data);
            
            // get chart for the "key" stream
            var chart=$("#global_chart").highcharts();

            // get current chart values
            var chart_data = chart.series[0].data;

            // calculate new values
            // incoming_data is a dictionary of car_model : count
            // it will combine the data from all replica streams
            var incoming_data = {};

            // Iterate over incoming dictionary to sum the data from each replica
            $.each(loaded_data,function(key,value){
                for (var car_model in value){
                    if (incoming_data[car_model]){
                        incoming_data[car_model] += value[car_model];
                    }else{
                         incoming_data[car_model] = value[car_model];
                    }
                }
            })

            // combines the incoming data with the existing chart data
            var new_data = {}
            for (var i = 0; i < chart_data.length; i++) {
                car_model = chart_data[i].category;
                current_value = chart_data[i].y;
                incoming_value = incoming_data[car_model];
                if (incoming_value){
                    if(sum){
                        new_value = current_value + incoming_value;
                    }else{
                        new_value = incoming_value;
                    }
                }else{
                    if(sum){
                        new_value = current_value;
                    }else{
                        new_value = 0;
                    }
                }
                new_data[car_model] = new_value;
                delete incoming_data[car_model];
            }

            // then get the count for remaining car models that were not in the chart
            for (var car_model in incoming_data){
                new_data[car_model] = incoming_data[car_model];                        
            }

            // convert the dict into an array of objects
            var new_data_array = []
            for(var car_model in new_data){
                new_data_array.push({"car_model":car_model,"count":new_data[car_model]})
            }

            // sort the new data array
            new_data_array.sort(function(a,b){
                var x = a["car_model"]; var y = b["car_model"];
                return ((x < y) ? -1 : ((x > y) ? 1 : 0));
            })

            // transform to two arrays (for x and y)
            var categories = []
            var data = []
            for (var k = 0; k < new_data_array.length; k++) {
                categories[k] = new_data_array[k]["car_model"];
                data[k] = new_data_array[k]["count"];
            }

            // update chart data
            chart.xAxis[0].setCategories(categories);
            chart.series[0].setData(data);  
            setTimeout(function(){
                update_global_chart(sum);
              }, TARGET_POLL_RATE * 1000);
        }
    });
}


$( ".show_chart" ).click(function() {
  var country = $(this).attr("id").split('_')[0];
  var country_chart = $("#"+country+"_chart");
  if(country_chart.is(":hidden")){
    country_chart.show();
  }else{
    country_chart.hide();
  }
});

$( "#global_cluster" ).click(function() {
  if($("#global_chart").is(":hidden")){
    $("#global_chart").show();
  }else{
    $("#global_chart").hide();
  }
});

function create_charts(){
    $(".country_chart").each(function(){
        create_chart($(this).data("country"));
    })
}

$( document ).ready(function(){
  display_streams();
  create_charts();
  update_country_charts(true); // true means that we sum the incoming counts in the chart
  update_global_chart(true); // true means that we sum the incoming counts in the chart
});
