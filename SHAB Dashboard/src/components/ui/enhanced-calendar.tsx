import * as React from "react"
import { ChevronLeft, ChevronRight } from "lucide-react"
import { DayPicker } from "react-day-picker"
import { 
  format, 
  addMonths, 
  addYears, 
  setMonth, 
  setYear
} from "date-fns"

import { cn } from "@/lib/utils"
import { buttonVariants } from "@/components/ui/button-variants"

export type CalendarProps = React.ComponentProps<typeof DayPicker>

function EnhancedCalendar({ className, classNames, showOutsideDays = true, ...props }: CalendarProps) {
  const [view, setView] = React.useState<'days' | 'months' | 'years'>('days')
  
  // Initialize with prop or default
  const [currentMonth, setCurrentMonth] = React.useState<Date>(
    props.month || props.defaultMonth || new Date()
  )
  
  const { onMonthChange, ...otherProps } = props

  // Sync with prop if it changes (controlled mode support)
  React.useEffect(() => {
    if (props.month) {
      setCurrentMonth(props.month)
    }
  }, [props.month])

  const handlePrevious = () => {
    if (view === 'days') {
      const newDate = addMonths(currentMonth, -1)
      setCurrentMonth(newDate)
      onMonthChange?.(newDate)
    } else if (view === 'months') {
      setCurrentMonth(addYears(currentMonth, -1))
    } else if (view === 'years') {
      setCurrentMonth(addYears(currentMonth, -10))
    }
  }

  const handleNext = () => {
    if (view === 'days') {
      const newDate = addMonths(currentMonth, 1)
      setCurrentMonth(newDate)
      onMonthChange?.(newDate)
    } else if (view === 'months') {
      setCurrentMonth(addYears(currentMonth, 1))
    } else if (view === 'years') {
      setCurrentMonth(addYears(currentMonth, 10))
    }
  }

  const handleTitleClick = () => {
    if (view === 'days') setView('months')
    else if (view === 'months') setView('years')
  }

  const handleMonthSelect = (monthIndex: number) => {
    const newDate = setMonth(currentMonth, monthIndex)
    setCurrentMonth(newDate)
    setView('days')
    onMonthChange?.(newDate)
  }

  const handleYearSelect = (year: number) => {
    const newDate = setYear(currentMonth, year)
    setCurrentMonth(newDate)
    setView('months')
    onMonthChange?.(newDate)
  }

  const getHeaderText = () => {
    if (view === 'days') return format(currentMonth, 'MMMM yyyy')
    if (view === 'months') return format(currentMonth, 'yyyy')
    if (view === 'years') {
      const start = Math.floor(currentMonth.getFullYear() / 10) * 10
      return `${start} - ${start + 9}`
    }
    return ''
  }

  return (
    <div className={cn("p-3 min-w-[300px]", className)}>
      {/* Custom Header */}
      <div className="flex justify-between items-center mb-4">
        <button
          onClick={handlePrevious}
          className={cn(
            buttonVariants({ variant: "outline" }),
            "h-7 w-7 bg-transparent p-0 opacity-50 hover:opacity-100"
          )}
        >
          <ChevronLeft className="h-4 w-4" />
        </button>
        
        <button 
          onClick={handleTitleClick}
          className="text-sm font-medium hover:bg-accent hover:text-accent-foreground px-2 py-1 rounded-md transition-colors"
        >
          {getHeaderText()}
        </button>

        <button
          onClick={handleNext}
          className={cn(
            buttonVariants({ variant: "outline" }),
            "h-7 w-7 bg-transparent p-0 opacity-50 hover:opacity-100"
          )}
        >
          <ChevronRight className="h-4 w-4" />
        </button>
      </div>

      {/* Views */}
      {view === 'days' && (
        <DayPicker
          showOutsideDays={showOutsideDays}
          className={cn("p-0")}
          month={currentMonth}
          onMonthChange={(date) => {
            setCurrentMonth(date)
            onMonthChange?.(date)
          }}
          classNames={{
            months: "flex flex-col sm:flex-row space-y-4 sm:space-x-4 sm:space-y-0",
            month: "space-y-4",
            caption: "hidden", // Hide default caption
            caption_label: "hidden",
            nav: "hidden", // Hide default nav
            table: "w-full border-collapse space-y-1",
            head_row: "flex",
            head_cell: "text-muted-foreground rounded-md w-9 font-normal text-[0.8rem]",
            row: "flex w-full mt-2",
            cell: "h-9 w-9 text-center text-sm p-0 relative [&:has([aria-selected].day-range-end)]:rounded-r-md [&:has([aria-selected].day-outside)]:bg-accent/50 [&:has([aria-selected])]:bg-accent first:[&:has([aria-selected])]:rounded-l-md last:[&:has([aria-selected])]:rounded-r-md focus-within:relative focus-within:z-20",
            day: cn(
              buttonVariants({ variant: "ghost" }),
              "h-9 w-9 p-0 font-normal aria-selected:opacity-100"
            ),
            day_range_end: "day-range-end",
            day_selected:
              "bg-primary text-primary-foreground hover:bg-primary hover:text-primary-foreground focus:bg-primary focus:text-primary-foreground",
            day_today: "bg-accent text-accent-foreground",
            day_outside:
              "day-outside text-muted-foreground opacity-50 aria-selected:bg-accent/50 aria-selected:text-muted-foreground aria-selected:opacity-30",
            day_disabled: "text-muted-foreground opacity-50",
            day_range_middle:
              "aria-selected:bg-accent aria-selected:text-accent-foreground",
            day_hidden: "invisible",
            ...classNames,
          }}
          components={{
            IconLeft: ({ ..._props }) => <ChevronLeft className="h-4 w-4" />,
            IconRight: ({ ..._props }) => <ChevronRight className="h-4 w-4" />,
          }}
          {...otherProps}
        />
      )}

      {view === 'months' && (
        <div className="grid grid-cols-4 gap-2">
          {Array.from({ length: 12 }).map((_, i) => (
            <button
              key={i}
              onClick={() => handleMonthSelect(i)}
              className={cn(
                "h-10 w-full rounded-md text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground",
                currentMonth.getMonth() === i && "bg-primary text-primary-foreground hover:bg-primary hover:text-primary-foreground"
              )}
            >
              {format(new Date(2000, i, 1), 'MMM')}
            </button>
          ))}
        </div>
      )}

      {view === 'years' && (
        <div className="grid grid-cols-4 gap-2">
          {(() => {
            const currentYear = currentMonth.getFullYear()
            const startYear = Math.floor(currentYear / 10) * 10
            const years = Array.from({ length: 12 }, (_, i) => startYear - 1 + i) // Show 1 before and 1 after logic? Standard is usually 10.
            // Windows shows 2020-2029 (10 years). Let's do 12 for grid 4x3.
            // 4x3 = 12. Let's show startYear - 1 to startYear + 10
            
            return years.map((year) => (
              <button
                key={year}
                onClick={() => handleYearSelect(year)}
                className={cn(
                  "h-10 w-full rounded-md text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground",
                  currentMonth.getFullYear() === year && "bg-primary text-primary-foreground hover:bg-primary hover:text-primary-foreground",
                  (year < startYear || year > startYear + 9) && "text-muted-foreground opacity-50"
                )}
              >
                {year}
              </button>
            ))
          })()}
        </div>
      )}
    </div>
  )
}

EnhancedCalendar.displayName = "EnhancedCalendar"

export { EnhancedCalendar }
