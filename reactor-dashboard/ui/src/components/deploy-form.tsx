"use client";

import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import * as z from "zod";
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  Sheet,
  SheetClose,
  SheetContent,
  SheetDescription,
  SheetFooter,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet"
import {
  Select,
  SelectContent,
  SelectGroup,
  SelectItem,
  SelectLabel,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea"

// Schema to validate JSON
const jsonSchema = z.object({
  jsonText: z
    .string()
    .refine(
      (val) => {
        try {
          JSON.parse(val);
          return true;
        } catch {
          return false;
        }
      },
      { message: "Invalid JSON" }
    ),
});

type FormValues = z.infer<typeof jsonSchema>;


function SelectOp() {
  return (
    <Select>
      <SelectTrigger className="w-[180px]">
        <SelectValue placeholder="Select Op" />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          <SelectLabel>Op</SelectLabel>
          <SelectItem value="apple">Apple</SelectItem>
          <SelectItem value="banana">Banana</SelectItem>
          <SelectItem value="blueberry">Blueberry</SelectItem>
          <SelectItem value="grapes">Grapes</SelectItem>
          <SelectItem value="pineapple">Pineapple</SelectItem>
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}


function PlaceOn() {
  return (
    <Select>
      <SelectTrigger className="w-[180px]">
        <SelectValue placeholder="Place On" />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          <SelectLabel>Node</SelectLabel>
          <SelectItem value="apple">Apple</SelectItem>
          <SelectItem value="banana">Banana</SelectItem>
          <SelectItem value="blueberry">Blueberry</SelectItem>
          <SelectItem value="grapes">Grapes</SelectItem>
          <SelectItem value="pineapple">Pineapple</SelectItem>
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}

function RemovePlacement() {
  return (
    <Select>
      <SelectTrigger className="w-[180px]">
        <SelectValue placeholder="Unplace From" />
      </SelectTrigger>
      <SelectContent>
        <SelectGroup>
          <SelectLabel>Node</SelectLabel>
          <SelectItem value="apple">Apple</SelectItem>
          <SelectItem value="banana">Banana</SelectItem>
          <SelectItem value="blueberry">Blueberry</SelectItem>
          <SelectItem value="grapes">Grapes</SelectItem>
          <SelectItem value="pineapple">Pineapple</SelectItem>
        </SelectGroup>
      </SelectContent>
    </Select>
  )
}

export default function DialogDemo() {
 return (
    <Sheet>
      <SheetTrigger asChild>
        <Button>Deploy Job</Button>
      </SheetTrigger>
      <SheetContent className="w-[1000px] sm:w-[1000px]">
        <SheetHeader>
          <SheetTitle>Deploy Job</SheetTitle>
        </SheetHeader>
        <div className="grid flex-1 auto-rows-min gap-6 px-4">
          <SelectOp/>

          <div className="grid gap-3">
            <Label htmlFor="oparg-json">Op Arg JSON</Label>
            <Textarea id="oparg-json" name="oparg_json" defaultValue="{}" required/>
          </div>

          <div className="flex items-center space-x-2">
            <PlaceOn />
            <Input type="number" placeholder="1" className="w-20" />
            <Button>Place</Button>
          </div>

          <div className="flex items-center space-x-2 w-full">
            <RemovePlacement className="flex-1" />
            <Button variant="destructive">UnPlace</Button>
          </div>

          <div className="grid gap-3">
            <Label htmlFor="job-json">Job JSON</Label>
            <Textarea id="job-json" name="job_json" defaultValue="{}" disabled required/>
          </div>

          <div className="grid gap-3">
            <Label htmlFor="job-name">Name</Label>
            <Input id="job-name" name="job_name" defaultValue="" />
          </div>

        </div>
        <SheetFooter>
          <Button variant="secondary">Load</Button>
          <Button variant="secondary">Save changes</Button>
          <Button type="submit">Deploy</Button>
          <SheetClose asChild>
            <Button variant="outline">Close</Button>
          </SheetClose>
        </SheetFooter>
      </SheetContent>
    </Sheet>
  )
}
