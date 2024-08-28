$( "#createBtn" ).click(function() {
  $( "#accordion" ).accordion({
		active: 0
	}
)
  $( "#saveAfterCreateBtn" ).css("display", "");
  $( "#saveAfterEditBtn" ).css("display", "none");
  $("#isEdit input").val('Create').blur();
});


$( "#saveAfterCreateBtn" ).click(function() {
	$( "#accordion" ).accordion({
		active: 1
	})
});


$( "#editBtn" ).click(function() {
	
  $( "#accordion" ).accordion({
		active: 0
	}
)
  $( "#saveAfterEditBtn" ).css("display", "");
  $( "#saveAfterCreateBtn" ).css("display", "none");
  $("#isEdit input").val('Edit').blur();
});

$("#cancelBtn").click(function() {
  $( "#accordion" ).accordion({
		active: 1
	}
)
});

$(".admin-buttons :first-child").css({
	"background": "#fff",
	"border-radius":"4px",
	"letter-spacing": "0px"
}); 

$("[value='Delete ']").css({
	"border-color": "#CDCDCD",
	"letter-spacing": "0px"
}); 
$("[value='Activate ']").css({
	"border-color": "#CDCDCD",
	"letter-spacing": "0px"
}); 
$("[value='Deactivate ']").css({
	"border-color": "#CDCDCD",
	"letter-spacing": "0px"
}); 

$("[value='Export ']").css({
	"border-color": "#CDCDCD",
	"letter-spacing": "0px"
}); 

$("[value='Edit ']").css({
	"border-color": "#CDCDCD",
	"letter-spacing": "0px"
});

$("[value='Create']").css({
	"border-color": "#0074D9",
	"letter-spacing": "0px"
}); 
$("[value='Delete']").css({
	"cursor": "pointer",
	"letter-spacing": "0px"
}); 
$("[value='Activate']").css({
	"cursor": "pointer",
	"letter-spacing": "0px"
}); 
$("[value='Deactivate']").css({
	"cursor": "pointer",
	"letter-spacing": "0px"
}); 
$("[value='Export']").css({
	"cursor": "pointer",
	"letter-spacing": "0px"
}); 

$("[value='Edit']").css({
	"cursor": "pointer",
	"letter-spacing": "0px"
}); 

$("[value='Import']").css({
	"cursor": "pointer",
	"letter-spacing": "0px"
}); 


$("[value='Re-Apply Template']").css({
	"cursor": "pointer",
	"letter-spacing": "0px"
}); 
$("#createBtn :first-child").css({
	"background": "#0074D9",
	"border-radius":"4px",
	"letter-spacing": "1px",
	"cursor": "pointer",
	"letter-spacing": "0px"
});

isMarked = $("#isMarked").text().trim()
exportCount = $("#exportCount").text().trim()

if(isMarked == "True"){
	$("#modifyControls").css("display","")
	$("#modifyControlsDisabled").css("display","none")
	$("#exportDisable").css("display","none")


}else{
	$("#modifyControls").css("display","none")
	$("#modifyControlsDisabled").css("display","")
	


if(exportCount == "True"){
	$("#exportEnable").css("display","")
    $("#exportDisable").css("display","none")
}else{
	$("#exportEnable").css("display","none")
	$("#exportDisable").css("display","")
}
}