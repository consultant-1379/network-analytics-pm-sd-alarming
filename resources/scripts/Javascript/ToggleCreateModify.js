/*$("#CreateCollectionButton").click(function(){
  $("#msg").hide();
  $("#create-window").show();
  $("#home-window").hide();
});



$("#cancel-create").click(function(){
  $("#msg").hide();
  $("#create-window").hide();
  $("#home-window").show();
});


$("#cancel-modify").click(function(){
  $("#msg").hide();
  $("#modifyCollection").hide();
  $("#home-window").show();
});



$("#ModifyCollectionButton").click(function(){
  $("#modifyCollection").show();
  $("#msg").hide();
   $("#home-window").hide();
  $("#create-window").hide();
});*/

var inputVal = $("#createModify").text().trim()
var isMarked = $("#isMarkedCheck").text().trim()



if (inputVal == "Modify Collection"){
	  $("#create-window").hide();
	  $("#modifyCollection").show();
	}
else if (inputVal == "Create Collection"){
	$("#create-window").show();
	$("#modifyCollection").hide();
	}
else{
	$("#create-window").hide();
	$("#modifyCollection").hide();
	}	
	
if(isMarked == "None"){
	$("#ModifyCollection").hide();
	$("#ModifyCollectionDisabled").show();
}else if(isMarked == "Marked"){
	$("#ModifyCollection").show();
	$("#ModifyCollectionDisabled").hide();
}




$("[value='Edit']").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"cursor": "pointer"
}); 


$("[value='Delete']").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"cursor": "pointer"
}); 

$("[value='Edit ']").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"border-color": "#CDCDCD"
}); 


$("[value='Delete ']").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"border-color": "#CDCDCD"
}); 




$("#modifyCollection :first-child").css({
	//"background-image": "linear-gradient(to bottom, #0074D9, #0074D9)",
	"border-radius":"4px",
	"letter-spacing": "0px"
}); 

$("#CreateCollectionButton :first-child").css({
	"background": "#0074D9",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"border-color": "#0074D9",
	"cursor": "pointer"
}); 

$("#saveBtn :first-child").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"cursor": "pointer"
}); 

$("#saveChangesBtn :first-child").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"cursor": "pointer"
}); 
$("#cancel-create :first-child").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"cursor": "pointer"
}); 

$("#cancel-modify :first-child").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px",
	"cursor": "pointer"
}); 



