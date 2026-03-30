import { z } from 'zod';

// ─── Kafka topic name ──────────────────────────────────────────────────────────
export const CHECKIN_TOPIC = 'checkin-events' as const;

// ─── Valid workout types (from checkin CSV) ────────────────────────────────────
export const WorkoutTypeSchema = z.enum([
  'Weightlifting',
  'Swimming',
  'Pilates',
  'CrossFit',
  'Yoga',
  'Cardio',
]);
export type WorkoutType = z.infer<typeof WorkoutTypeSchema>;

// ─── The canonical event shape published to Kafka ─────────────────────────────
// Every producer MUST conform to this. Every consumer validates against this.
export const CheckinEventSchema = z.object({
  eventId: z.uuid(), // unique per event
  userId: z.string().min(1), // e.g. "user_1"
  gymId: z.string().min(1), // e.g. "gym_3"
  checkinTime: z.iso.datetime(), // ISO 8601
  checkoutTime: z.iso.datetime(), // ISO 8601
  workoutType: WorkoutTypeSchema,
  caloriesBurned: z.number().positive(),
  durationMins: z.number().positive(),
  source: z.enum(['csv-replay', 'simulator', 'live']),
});

export type CheckinEvent = z.infer<typeof CheckinEventSchema>;

// ─── Member behavioral profile stored in Redis ────────────────────────────────
// Key: member:profile:{userId}
// Written by Consumer Group 1 (profile-updater)
// Read by the recommendation engine
export const MemberProfileSchema = z.object({
  userId: z.string(),
  totalCheckins: z.number().int().nonnegative(),
  lastCheckinAt: z.iso.datetime().nullable(),

  // Workout type affinity — count per type, used to compute preference score
  workoutCounts: z.record(WorkoutTypeSchema, z.number().int().nonnegative()),

  // Hour-of-day histogram — how many checkins at each hour (0–23)
  hourHistogram: z.record(z.string(), z.number().int().nonnegative()),

  // Rolling averages (last 30 events)
  avgDurationMins: z.number().nonnegative(),
  avgCalories: z.number().nonnegative(),

  subscriptionPlan: z.enum(['Basic', 'Pro', 'Student']),
  updatedAt: z.iso.datetime(),
});

export type MemberProfile = z.infer<typeof MemberProfileSchema>;

// ─── Recommendation result returned by the API ────────────────────────────────
export const RecommendationSchema = z.object({
  workoutType: WorkoutTypeSchema,
  score: z.number().min(0).max(1), // normalised 0–1
  suggestedHour: z.number().int().min(0).max(23),
  reasoning: z.object({
    affinityScore: z.number(), // how much user likes this workout type
    timeScore: z.number(), // alignment with preferred hours
    facilityMatch: z.boolean(), // gym has required facility
    calorieScore: z.number(), // alignment with user's avg calorie burn
  }),
});

export type Recommendation = z.infer<typeof RecommendationSchema>;