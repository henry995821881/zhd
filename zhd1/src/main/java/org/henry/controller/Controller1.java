package org.henry.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class Controller1 {

	
	
	
	@RequestMapping("/test")
	public String Show(Model model){
		
		
		
		
		model.addAttribute("hello", "world");
		
		return "test";
	}
	
	
}
