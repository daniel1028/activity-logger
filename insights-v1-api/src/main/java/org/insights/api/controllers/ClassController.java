package org.insights.api.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping(value="/")
public class ClassController{
	
	@RequestMapping(value="/class/text",method ={ RequestMethod.GET,RequestMethod.POST})
	@ResponseBody
	public String getClassUnitUsage(HttpServletRequest request, 
			 HttpServletResponse response) throws Exception{
		return "Its workingggg";
	}
	
}
	
